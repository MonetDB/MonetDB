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

      elem
      attr 
      text
      doc 
      comment
      pi  
      tag

      true_
      false_
      atomize
      error
      root_
      empty_
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
      Atom
      NonAtom
      FunctionAppl
      FunctionArgs
      BuiltIns
      LiteralValue
      LiteralString
      ConstructorExpr
      TagName
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
NonAtom:         FunctionAppl;
NonAtom:         BuiltIns;
NonAtom:         ConstructorExpr;

BindingExpr:     for_ (var_, OptVar, CoreExpr, CoreExpr)
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
         * Given the updated environment and the new loop relation
         * compile e2. Return the (possibly intermediate) result.
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
                     project (rownum ([[ $3$ ]].result,
                                      "inner",
                                      sortby ("iter", "pos"),
                                      NULL),
                              proj ("iter", "inner"),
                              proj ("item", "item")));

        /* save old environment */
        old_env = env;
        env = PFarray (sizeof (PFalg_env_t));

        /* insert $v and "its document" into NEW environment */
        *((PFalg_env_t *) PFarray_add (env)) = enventry ($1$->sem.var,
                                                         var, [[ $3$ ]].doc);

        /* save old loop operator */
        old_loop = loop;

        /* create new loop operator */
        loop = project (var, proj ("iter", "iter"));

        /* create map relation. */
        map = project (rownum([[ $3$ ]].result,
                              "inner",
                              sortby ("iter", "pos"),
                              NULL),
                 proj ("outer", "iter"),
                 proj ("inner", "inner"));

        /*
         * handle optional variable ($p); we need map operator
         * for this purpose
         * note that the rownum () routine is used to create
         * the 'item' column of $p's operator; since this
         * column must be of type integer instead of nat, we
         * cast it accordingly
         */
        if ($2$->sem.var) {
            opt_var = cross (lit_tbl (attlist ("pos"),
                                      tuple (lit_nat (1))),
                             cast (project (rownum (map, "item",
                                                    sortby ("inner"),
                                                    "outer"),
                                            proj ("iter", "inner"),
                                            proj ("item", "item")),
                                   "item", aat_int));

            /* insert $p into NEW environment */
            *((PFalg_env_t *) PFarray_add (env)) =
                enventry ($2$->sem.var, opt_var, PFalg_empty_frag ());
        }

        /* update all variable bindings in old environment and put
         * them into new environment */
        for (i = 0; i < PFarray_last (old_env); i++) {
            e = *((PFalg_env_t *) PFarray_at (old_env, i));
            new_bind = project (eqjoin (e.result, map, "iter", "outer"),
                               proj ("iter", "inner"),
                               proj ("pos", "pos"),
                               proj ("item", "item"));
            *((PFalg_env_t *) PFarray_add (env)) =
                enventry (e.var, new_bind, e.doc);
        }

        /* translate e2 under the specified conditions (updated
         * environment, loop(v), delta1)
         */
        tDO($%3$);

        /* restore old loop */
        loop = old_loop;

        /* restore old environment */
        env = old_env;

        /* compute result using old env and old loop. */
        [[ $$ ]] =(struct PFalg_pair_t) {
                 .result = project (rownum (eqjoin([[ $4$ ]].result,
                                                   map, "iter", "inner"),
                                            "pos1",
                                            sortby ("iter", "pos"),
                                            "outer"),
                                    proj ("iter", "outer"),
                                    proj ("pos", "pos1"),
                                    proj ("item", "item")),
                 .doc = [[ $4$ ]].doc };
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
         * environment together with the currently live nodes
         */
         *((PFalg_env_t *) PFarray_add (env)) = enventry ($1$->sem.var,
                                                          [[ $2$ ]].result,
                                                          [[ $2$ ]].doc);

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
    { TOPDOWN; }
    =
    {
        /*
         * CoreExpr1 is the expression to be switched. CoreExpr2 
         * compiles one (the current) case branch. CoreExpr3 is
         * either another typeswitch representing the next case
         * branch or the default branch of the overall typeswitch.
         *
         * A lot of work for this translation is captured in the
         * function type_test() in core2alg_impl.c. Given an algebra
         * expression and an XQuery sequence type, it will return
         * a relation with columns `iter' and `subty', with `subty'
         * set to true or false, depending on whether for this
         * iteration the sequence type test succeeds or not.
         *
         * env,loop: e1 => q1,delta1
         * tested_q1 = type_test (ty, q1, loop)
         * 
         * -- translate stuff in the `case' branch
         *  loop2 = proj_iter (select_subty (tested_q1))
         *  {..., $v -> proj_iter,pos,item (
         *    qv |X| (iter = iter1) (proj_iter1:iter loop2))},
         *   loop2: e2 => q2,delta2
         * 
         * -- and in the `default' branch
         *  loop3 = proj_iter (select_notsub (not_notsub:subty (tested_q1)))
         *  {..., $v -> proj_iter,pos,item (
         *    qv |X| (iter = iter1) (proj_iter1:iter loop3))},
         *   loop3: e3 => q3,delta3
         * 
         * ---------------------------------------------------------------
         *  env,loop:
         *  typeswitch (e1) case ty return e2 default return e3 =>
         *    (q2 U q3, delta2 U delta3)
         *
         * NB: the TYPE operator creates a new column of type boolean;
         * it examines whether the specified column is of given type "ty";
         * if this is the case, it sets the new column to true, otherwise
         * to false.
         */

        PFalg_op_t   *tested_q1;  /* true/false if iteration satisfies test */
        PFarray_t    *old_env;    /* backup of surrounding environment */
        PFalg_op_t   *old_loop;   /* backup of surrounding loop relation */
        PFalg_env_t   e;          /* helper variable */
        PFalg_op_t   *new_bind;   /* helper variable */
        unsigned int  i;

        /* translate CoreExpr1 */
        tDO ($%1$);

        tested_q1 = type_test ($2.1.1$->sem.type, [[ $1$ ]], loop);

        /* translate stuff in the `case' branch */

        /* map `loop' relation */
        old_loop = loop;
        loop = project (select_ (tested_q1, "subty"), proj ("iter", "iter"));

        /* map variable environment */
        old_env = env;
        env = PFarray (sizeof (PFalg_env_t));

        for (i = 0; i < PFarray_last (old_env); i++) {
            e = *((PFalg_env_t *) PFarray_at (old_env, i));
            new_bind =
                project (eqjoin (e.result,
                                 project (loop, proj ("iter1", "iter")),
                                 "iter", "iter1"),
                         proj ("iter", "iter"),
                         proj ("pos", "pos"),
                         proj ("item", "item"));
            *((PFalg_env_t *) PFarray_add (env))
                = enventry (e.var, new_bind, e.doc);
        }

        /* translate CoreExpr2 */
        tDO ($%2$);

        /* translate stuff in the `default' branch (equivalently) */

        /* map `loop' relation */
        loop = project (select_ (not (tested_q1, "notsub", "subty"), "notsub"),
                        proj ("iter", "iter"));

        env = PFarray (sizeof (PFalg_env_t));

        for (i = 0; i < PFarray_last (old_env); i++) {
            e = *((PFalg_env_t *) PFarray_at (old_env, i));
            new_bind =
                project (eqjoin (e.result,
                                 project (loop, proj ("iter1", "iter")),
                                 "iter", "iter1"),
                         proj ("iter", "iter"),
                         proj ("pos", "pos"),
                         proj ("item", "item"));
            *((PFalg_env_t *) PFarray_add (env))
                = enventry (e.var, new_bind, e.doc);
        }

        /* translate CoreExpr3 */
        tDO ($%3$);

        /* reset loop relation and environment */
        loop = old_loop;
        env = old_env;

        [[ $$ ]] = (struct PFalg_pair_t) {
            .result = disjunion ([[ $2.1.2$ ]].result, [[ $3$ ]].result),
            .doc    = PFalg_set_union ([[ $2.1.2$ ]].doc, [[ $3$ ]].doc)
        };
    }
    ;

SequenceTypeCast: seqcast (seqtype, CoreExpr)
    =
    {
        /*
         * `seqcast' nodes are only introduced for static typing.
         * They are not meant to be executed.
         */
        [[ $$ ]] = [[ $2$ ]];
    }
    ;

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
         * NB: SEL: select those rows where column value != 0
         *     
         *
         * {..., $v -> q(v), ...},loop,delta: e1 => q1,delta1
         * loop2 = proj_iter (SEL item q1)
         * loop3 = proj_iter (SEL res (NOT res item q1))
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
        loop = project (select_ ([[ $1$ ]].result, "item"),
                        proj ("iter", "iter"));

        /* save old environment */
        old_env = env;

        /* update the environment for translation of e2 */
        env = PFarray (sizeof (PFalg_env_t));

        for (i = 0; i < PFarray_last (old_env); i++) {
            e = *((PFalg_env_t *) PFarray_at (old_env, i));
            new_bind = project (eqjoin (e.result,
                                        project (loop,
                                                 proj ("iter1", "iter")),
                                        "iter",
                                        "iter1"),
                                proj ("iter", "iter"),
                                proj ("pos", "pos"),
                                proj ("item", "item"));
            *((PFalg_env_t *) PFarray_add (env)) =
                enventry (e.var, new_bind, e.doc);
        }

        /* translate e2 */
        tDO($%2$);

        /* create loop3 operator */
        loop = project (select_ (not ([[ $1$ ]].result,
                                     "res",
                                     "item"),
                                "res"),
                        proj ("iter", "iter"));

        /* update the environment for translation of e3 */
        env = PFarray (sizeof (PFalg_env_t));

        for (i = 0; i < PFarray_last (old_env); i++) {
            e = *((PFalg_env_t *) PFarray_at (old_env, i));
            new_bind = project (eqjoin (e.result,
                                        project (loop,
                                                 proj ("iter1", "iter")),
                                        "iter",
                                        "iter1"),
                                proj ("iter", "iter"),
                                proj ("pos", "pos"),
                                proj ("item", "item"));
            *((PFalg_env_t *) PFarray_add (env)) =
                enventry (e.var, new_bind, e.doc);
        }

        /* translate e3 */
        tDO($%3$);

        /* reset loop relation and environment */
        loop = old_loop;
        env = old_env;

        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = disjunion ([[ $2$ ]].result, [[ $3$ ]].result),
                 .doc = PFalg_set_union ([[ $2$ ]].doc, [[ $3$ ]].doc) };
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
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = project (
                     rownum (
                         disjunion (cross (lit_tbl (attlist ("ord"),
                                                    tuple (lit_nat (1))),
                                           [[ $1$ ]].result),
                                    cross (lit_tbl (attlist ("ord"),
                                                    tuple (lit_nat (2))),
                                           [[ $2$ ]].result)),
                         "pos1", sortby ("ord", "pos"), "iter"),
                     proj ("iter", "iter"),
                     proj ("pos", "pos1"),
                     proj ("item", "item")),
                 .doc = PFalg_set_union ([[ $1$ ]].doc, [[ $2$ ]].doc) };
    }
    ;

PathExpr:        LocationSteps;
PathExpr:        LocationStep;

LocationStep:    ancestor (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = anc ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    ancestor_or_self (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = anc_self ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    attribute (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = attr ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    child_ (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = child ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    descendant (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = desc ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    descendant_or_self (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = desc_self ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    following (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = fol ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    following_sibling (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = fol_sibl ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    parent_ (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = par ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    preceding (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = prec ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    preceding_sibling (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = prec_sibl ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
    }
    ;
LocationStep:    self (NodeTest)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = self ([[ $1$ ]].result),
                                            .doc = PFalg_empty_frag () };
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
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = rownum (scjoin (PFalg_alg_union ([[ $2$ ]].doc),
                                           project ([[ $2$ ]].result,
                                                    proj ("iter", "iter"),
                                                    proj ("item", "item")),
                                           [[ $1$ ]].result),
                                   "pos", sortby ("item"), "iter"),
                 .doc = [[ $2$ ]].doc};
    }
    ;

NodeTest:        namet
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = nameTest ($$->sem.qname),
                                            .doc = PFalg_empty_frag () };
    }
    ;
NodeTest:        KindTest;

KindTest:        kind_node (nil)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = nodeTest (),
                                            .doc = PFalg_empty_frag () };
    }
    ;
KindTest:        kind_comment (nil)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = commTest (),
                                            .doc = PFalg_empty_frag () };
    }
    ;
KindTest:        kind_text (nil)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = textTest (),
                                            .doc = PFalg_empty_frag () };
    }
    ;
KindTest:        kind_pi (nil)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = piTest (),
                                            .doc = PFalg_empty_frag () };
    }
    ;
KindTest:        kind_pi (lit_str)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = pitarTest ($1$->sem.str),
                                            .doc = PFalg_empty_frag () };
    }
    ;
KindTest:        kind_doc (nil)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = docTest (),
                                            .doc = PFalg_empty_frag () };
    }
    ;
KindTest:        kind_elem (nil)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = elemTest (),
                                            .doc = PFalg_empty_frag () };
    }
    ;
KindTest:        kind_attr (nil)
    =
    {
        [[ $$ ]] = (struct  PFalg_pair_t) { .result = attrTest (),
                                            .doc = PFalg_empty_frag () };
    }
    ;

ConstructorExpr: elem (TagName, CoreExpr)
    =
    {
        /*
         * CoreExpr (q2) evaluates to a sequence of nodes. TagName (q1)
         * is the name of a new node which becomes the common root of
         * the constructed nodes.
         *
         * env, loop: e1 => q1, doc (q1)
         * env, loop: e2 => q2, doc (q2)
         *
         * n = element (doc (q2), q1, q2)
         * ------------------------------------------------------------------
         * env, loop: element e1 {e2} =>
         *                                               pos
         * result:    (proj_iter,item:pre (roots (n))) x -----
         *                                                1
         *                                                          zero
         *    where roots (n) = sel (res)(= res:(level, zero) (n x ------))
         *                                                            0
         *
         * doc:        proj_pre,size,level,kind,prop,frag (n)
         */
        PFalg_op_t *elem = element (PFalg_alg_union ([[ $2$ ]].doc),
                                    [[ $1$ ]].result,
                                    [[ $2$ ]].result);

        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (project (
                                      select_ (
                                          eq (cross (elem,
                                                     lit_tbl (
                                                         attlist ("zero"),
                                                         tuple (
                                                             lit_int (0)))),
                                              "res","level","zero"),
                                          "res"),
                                      proj ("iter", "iter"),
                                      proj ("item", "pre")),
                                  lit_tbl (attlist ("pos"),
                                           tuple (lit_nat (1)))),
                 .doc = PFalg_new_frag (project (elem,
                                                 proj ("pre", "pre"),
                                                 proj ("size", "size"),
                                                 proj ("level", "level"),
                                                 proj ("kind", "kind"),
                                                 proj ("prop", "prop"),
                                                 proj ("frag", "frag"))) };
    }
    ;

ConstructorExpr: attr (TagName, CoreExpr)
    =
    {
        /*
         * CoreExpr (q2) evaluates to a sequence of attributes. TagName
         * (q1) is the name of a new node which becomes the common root
         * of the constructed attributes.
         *
         * env, loop: e1 => q1, doc (q1)
         * env, loop: e2 => q2, doc (q2)
         *
         * n = attribute (doc (q2), q1, q2)
         * ------------------------------------------------------------------
         * env, loop: attribute e1 {e2} =>
         *                                               pos
         * result:    (proj_iter,item:pre (roots (n))) x -----
         *                                                1
         *                                                          zero
         *    where roots (n) = sel (res)(= res:(level, zero) (n x ------))
         *                                                            0
         *
         * doc:        proj_pre,size,level,kind,prop,frag (n)
         */
        PFalg_op_t *attr = attribute (PFalg_alg_union ([[ $2$ ]].doc),
                                      [[ $1$ ]].result,
                                      [[ $2$ ]].result);

        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (project (
                                      select_ (
                                          eq (cross (attr,
                                                     lit_tbl (
                                                         attlist ("zero"),
                                                         tuple (
                                                             lit_int (0)))),
                                              "res","level","zero"),
                                          "res"),
                                      proj ("iter", "iter"),
                                      proj ("item", "pre")),
                                  lit_tbl (attlist ("pos"),
                                           tuple (lit_nat (1)))),
                 .doc = PFalg_new_frag (project (attr,
                                                 proj ("pre", "pre"),
                                                 proj ("size", "size"),
                                                 proj ("level", "level"),
                                                 proj ("kind", "kind"),
                                                 proj ("prop", "prop"),
                                                 proj ("frag", "frag"))) };
    }
    ;

ConstructorExpr: text (CoreExpr)
    =
    {
        /*
         * env, loop: e => q, doc (q)
         *
         * n = textnode (doc (q), q)
         * ------------------------------------------------------------------
         * env, loop: textnode e =>
         *                                               pos
         * result:    (proj_iter,item:pre (roots (n))) x -----
         *                                                1
         *                                                          zero
         *    where roots (n) = sel (res)(= res:(level, zero) (n x ------))
         *                                                            0
         *
         * doc:        proj_pre,size,level,kind,prop,frag (n)
         */
        PFalg_op_t *textnode = textnode ([[ $1$ ]].result);

        /* we do not have to check for 'level' == 0, since newly
         * created text nodes always have that level
         */
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (project (textnode,
                                           proj ("iter", "iter"),
                                           proj ("item", "pre")),
                                  lit_tbl (attlist ("pos"),
                                           tuple (lit_nat (1)))),
                 .doc = PFalg_new_frag (project (textnode,
                                                 proj ("pre", "pre"),
                                                 proj ("size", "size"),
                                                 proj ("level", "level"),
                                                 proj ("kind", "kind"),
                                                 proj ("prop", "prop"),
                                                 proj ("frag", "frag"))) };
    }
    ;

ConstructorExpr: doc (CoreExpr)
    =
    {
        /*
         * env, loop: e => q, doc (q)
         *
         * n = textnode (doc (q), q)
         * ------------------------------------------------------------------
         * env, loop: docnode e =>
         *                                               pos
         * result:    (proj_iter,item:pre (roots (n))) x -----
         *                                                1
         *                                                          zero
         *    where roots (n) = sel (res)(= res:(level, zero) (n x ------))
         *                                                            0
         *
         * doc:        proj_pre,size,level,kind,prop,frag (n)
         */
        PFalg_op_t *docnode = docnode (PFalg_alg_union ([[ $1$ ]].doc),
                                       [[ $1$ ]].result);

        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (project (
                                      select_ (
                                          eq (cross (docnode,
                                                     lit_tbl (
                                                         attlist ("zero"),
                                                         tuple (
                                                             lit_int (0)))),
                                              "res","level","zero"),
                                          "res"),
                                      proj ("iter", "iter"),
                                      proj ("item", "pre")),
                                  lit_tbl (attlist ("pos"),
                                           tuple (lit_nat (1)))),
                 .doc = PFalg_new_frag (project (docnode,
                                                 proj ("pre", "pre"),
                                                 proj ("size", "size"),
                                                 proj ("level", "level"),
                                                 proj ("kind", "kind"),
                                                 proj ("prop", "prop"),
                                                 proj ("frag", "frag"))) };
    }
    ;

ConstructorExpr: comment (lit_str)
    =
    {
        /*
         * env, loop: e => q, doc (q)
         *
         * n = comment (doc (q), q)
         * ------------------------------------------------------------------
         * env, loop: docnode e =>
         *                                               pos
         * result:    (proj_iter,item:pre (roots (n))) x -----
         *                                                1
         *                                                          zero
         *    where roots (n) = sel (res)(= res:(level, zero) (n x ------))
         *                                                            0
         *
         * doc:        proj_pre,size,level,kind,prop,frag (n)
         */
        PFalg_op_t *comment = comment ([[ $1$ ]].result);

        /* we do not have to check for 'level' == 0, since newly
         * created comments always have that level
         */
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (project (comment,
                                           proj ("iter", "iter"),
                                           proj ("item", "pre")),
                                  lit_tbl (attlist ("pos"),
                                           tuple (lit_nat (1)))),
                 .doc = PFalg_new_frag (project (comment,
                                                 proj ("pre", "pre"),
                                                 proj ("size", "size"),
                                                 proj ("level", "level"),
                                                 proj ("kind", "kind"),
                                                 proj ("prop", "prop"),
                                                 proj ("frag", "frag"))) };
    }
    ;

ConstructorExpr: pi (lit_str)
    =
    {
        /*
         * env, loop: e => q, doc (q)
         *
         * n = processi (doc (q), q)
         * ------------------------------------------------------------------
         * env, loop: docnode e =>
         *                                               pos
         * result:    (proj_iter,item:pre (roots (n))) x -----
         *                                                1
         *                                                          zero
         *    where roots (n) = sel (res)(= res:(level, zero) (n x ------))
         *                                                            0
         *
         * doc:        proj_pre,size,level,kind,prop,frag (n)
         */
        PFalg_op_t *pi = processi ([[ $1$ ]].result);

        /* we do not have to check for 'level' == 0, since newly
         * created processing instructions always have that level
         */
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (project (pi,
                                           proj ("iter", "iter"),
                                           proj ("item", "pre")),
                                  lit_tbl (attlist ("pos"),
                                           tuple (lit_nat (1)))),
                 .doc = PFalg_new_frag (project (pi,
                                                 proj ("pre", "pre"),
                                                 proj ("size", "size"),
                                                 proj ("level", "level"),
                                                 proj ("kind", "kind"),
                                                 proj ("prop", "prop"),
                                                 proj ("frag", "frag"))) };
    }
    ;

TagName:         tag;
TagName:         CoreExpr;

FunctionAppl:    apply (FunctionArgs)
    {
        /* This rule only applies for built-in functions. */
        if (! $$->sem.fun->builtin)
            ABORT;
    }
    =
    {
        /*
         * The XQuery semantics for arithmetics demands to cast both
         * operands of, say, op:numeric-add() to the most specific type
         * that both operands can be promoted to. This, however, means
         * that we cannot always choose the correct implementation at
         * compile time.
         *
         * (Consider
         *
         *     for $x in (1, 1.5) return
         *       for $y in (2, 2.5) return
         *         $x + $y  .
         *
         * This will produce four invocations of op:numeric-add(). The
         * first one will be integer, integer. The last one is a
         * decimal, decimal. In the middle two cases we need to cast one
         * operand to decimal and pick the decimal, decimal
         * implementation. Static typing does not give us sufficient
         * information here, but will pick the decimal, decimal
         * implementation for all cases.)
         *
         * Complying with the XQuery semantics would thus require to
         * have a polymorphic (`+') operator that can deal with all
         * the possible type combinations. This would not only make
         * translation quite ugly, but would probably hurt performance.
         *
         * After some discussions we thus agreed on an implementation
         * that hurts least but still stays reasonably close to the
         * XQuery semantics. After we decided on either of the possible
         * implementations of any built-in function, we cast all of its
         * arguments to the corresponding parameter type. In the above
         * example, this would cast all values to decimal, before
         * using the decimal, decimal implementation for all the four
         * cases.
         */
        /* unsigned int i; */

        if (!$$->sem.fun->alg)
            PFoops (OOPS_FATAL,
                    "Algebra implementation for function `%s' unknown.",
                    PFqname_str ($$->sem.fun->qname));

        /*
         * Cast parameters accordingly.
         *
         * foreach parameter p
         *   p := cast (p, "item", implementation type for this parameter)
         *
         */
        /*
        for (i = 0; i < PFarray_last ([[ $1$ ]]->sem.builtin.args); i++)
            *((PFalg_op_t **) PFarray_at ([[ $1$ ]]->sem.builtin.args, i))
                = cast (*((PFalg_op_t **)
                            PFarray_at ([[ $1$ ]]->sem.builtin.args, i)),
                        "item",
                        implty ($$->sem.fun->par_ty[i]));
        */


        [[ $$ ]] = $$->sem.fun->alg (loop,
                                     [[ $1$ ]].result->sem.builtin.args->base);
/*
        [[ $$ ]] = (struct  PFalg_pair_t) {
                         .result = $$->sem.fun->alg (loop,
                                   [[ $1$ ]].result->sem.builtin.args->base),
                         .doc = PFalg_empty_frag () };
*/
    }
    ;

FunctionAppl:    apply (FunctionArgs)
    {
        /* This rule only applies for user-defined functions. */
        if ($$->sem.fun->builtin)
            ABORT;
    }
    =
    { PFoops (OOPS_FATAL, "User-defined functions not implemented yet."); };

FunctionArgs:    arg (CoreExpr, FunctionArgs)
    =
    {
        /*
         * builds an array of 'result'/'doc' pairs and assigns it to
         * $$->arg.result->sem.builtin field; $$->arg.doc is always
         * empty
         */
        [[ $$ ]] = args ([[ $1$ ]], [[ $2$ ]]);
    }
    ;
FunctionArgs:    nil
    =
    {
        /*
         * creates a pair of algebra operators where 'result' and 'doc'
         * are NULL
         */
        [[ $$ ]] = args_tail ();
    }
    ;


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
                [[ $$ ]] = (struct  PFalg_pair_t) {
                         .result = e.result,
                         .doc = e.doc };
                break;
            }
        }
    }
    ;

Atom:            LiteralValue;

LiteralValue:    LiteralString;

LiteralString:    lit_str
    =
    {
        /*
         *  -------------------------------------------------------------
         *                          /        / pos | item \ \
         *  env, loop, delta: c => | loop X | -----+------ | |
         *                          \        \   1 |   c  / /
         */
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (loop,
                                  lit_tbl( attlist ("pos", "item"),
                                           tuple (lit_nat (1),
                                                  lit_str ($$->sem.str)))),
                 .doc = PFalg_empty_frag () };
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
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (loop,
                                  lit_tbl( attlist ("pos", "item"),
                                           tuple (lit_nat (1),
                                                  lit_int ($$->sem.num)))),
                 .doc = PFalg_empty_frag () };
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
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (loop,
                                  lit_tbl( attlist ("pos", "item"),
                                           tuple (lit_nat (1),
                                                  lit_dec ($$->sem.dec)))),
                 .doc = PFalg_empty_frag () };
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
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (loop,
                                  lit_tbl( attlist ("pos", "item"),
                                           tuple (lit_nat (1),
                                                  lit_dbl ($$->sem.dbl)))),
                 .doc = PFalg_empty_frag () };
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
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (loop,
                                  lit_tbl( attlist ("pos", "item"),
                                           tuple (lit_nat (1),
                                                  lit_bln (true)))),
                 .doc = PFalg_empty_frag () };
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
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (loop,
                                  lit_tbl( attlist ("pos", "item"),
                                           tuple (lit_nat (1),
                                                  lit_bln (false)))),
                 .doc = PFalg_empty_frag () };
    }
    ;
LiteralValue:    empty_
    =
    {
        /*
         *  -------------------------------------------------------------
         *                              iter | pos | item
         *  env, loop, delta: empty => ------+-----+------
         * 
         */

        /*
         * Some compilers (e.g., icc) don't like empty __VA_ARGS__
         * arguments, so we do not use the (more readable) lit_tbl()
         * macro here.
         *
         * [[ $$ ]] = lit_tbl (attlist ("iter", "pos", "item"));
         */
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = PFalg_lit_tbl_ (attlist ("iter", "pos", "item"),
                                           0, NULL),
                 .doc = PFalg_empty_frag () };
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
        [[ $$ ]] = (struct  PFalg_pair_t) {
                 .result = cross (loop,
                                  lit_tbl( attlist ("pos", "item"),
                                           tuple (lit_nat (1),
                                                  lit_nat (1)))),
                 .doc = PFalg_empty_frag () };
    }
    ;


/* vim:set shiftwidth=4 expandtab filetype=c: */
