/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue { 

/*
 * Map MIL algebra expressions to MIL code
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
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */
  
/* Auxiliary routines related to the translation are located in
 * this separate included file to facilitate automated documentation
 * via doxygen.
 */	
#include "milgen_impl.c"

};

node  serialize
      new_
      insert_
      seqbase
      project
      reverse
      sort
      ctrefine
      join
      leftjoin
      cross
      mirror
      kunique
      mark_grp
      mark
      count
      append
      oid
      moid
      mint
      madd

      lit_oid
      lit_int
      lit_str
      lit_bit
      lit_dbl
      ;

label Prog
      BatExpr
      AtomExpr
      Literal
      ;

Prog:        serialize (BatExpr, BatExpr, BatExpr, BatExpr,
                        BatExpr, BatExpr, BatExpr)
    { assert ($$);   /* avoid warning `unused variable: root' */ }
    =
    {
        assert ($1$->varname); assert ($2$->varname); assert ($3$->varname);
        assert ($4$->varname); assert ($5$->varname); assert ($6$->varname);
        assert ($7$->varname);

        execute (
            serialize (
                var ($1$->varname),    /* pos */
                var ($2$->varname),    /* item_int */
                var ($3$->varname),    /* item_str */
                var ($4$->varname),    /* item_dec */
                var ($5$->varname),    /* item_dbl */
                var ($6$->varname),    /* item_bln */
                var ($7$->varname)));  /* item_node */

        assert (_ll);  /* avoid warning `unused parameter: _ll' */
    }
    ;

BatExpr:     new_
    =
    {
        if ($$->varname)
            break;

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                 new (type ($$->sem.new.htype), type ($$->sem.new.ttype))));
    }
    ;

BatExpr:     insert_ (BatExpr, AtomExpr, AtomExpr)
    =
    {
        /*
         * FIXME: We should find something smarter than copying the
         *        BAT before we insert the tuple.
         */
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);
        assert ($2$ && $2$->varname);
        assert ($3$ && $3$->varname);

        $$->varname = new_var ();

        /*
         * If we share our child with some other operator, we need
         * to copy it first.
         */
        if ($$->refctr == $1$->refctr)
            execute (assgn (var ($$->varname),
                            access (insert (access (var ($1$->varname),
                                                    BAT_APPEND),
                                            var ($2$->varname),
                                            var ($3$->varname)),
                                     BAT_READ)));
        else
            execute (assgn (var ($$->varname),
                            access (insert (access (copy (var ($1$->varname)),
                                                    BAT_APPEND),
                                            var ($2$->varname),
                                            var ($3$->varname)),
                                    BAT_READ)));
    }
    ;


BatExpr:     seqbase (BatExpr, AtomExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);
        assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        /*
         * If we share our child with some other operator, we need
         * to copy it first.
         */
        if ($$->refctr == $1$->refctr)
            execute (assgn (var ($$->varname),
                            seqbase (var ($1$->varname), var ($2$->varname))));
        else
            execute (assgn (var ($$->varname),
                            seqbase (copy (var ($1$->varname)),
                                     var ($2$->varname))));
    }
    ;

BatExpr:     project (BatExpr, AtomExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);
        assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        project (var ($1$->varname), var ($2$->varname))));
    }
    ;

BatExpr:     reverse (BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname), reverse (var ($1$->varname))));
    }
    ;

BatExpr:     sort (BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname), sort (var ($1$->varname))));
    }
    ;

BatExpr:     ctrefine (BatExpr, BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);
        assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        ctrefine (var ($1$->varname), var ($2$->varname))));
    }
    ;

BatExpr:     join (BatExpr, BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);
        assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        join (var ($1$->varname), var ($2$->varname))));
    }
    ;

BatExpr:     leftjoin (BatExpr, BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);
        assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        leftjoin (var ($1$->varname), var ($2$->varname))));
    }
    ;

BatExpr:     cross (BatExpr, BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);
        assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        cross (var ($1$->varname), var ($2$->varname))));
    }
    ;

BatExpr:     mirror (BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname), mirror (var ($1$->varname))));
    }
    ;

BatExpr:     kunique (BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname), kunique (var ($1$->varname))));
    }
    ;

BatExpr:     mark_grp (BatExpr, BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);
        assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        mark_grp (var ($1$->varname), var ($2$->varname))));
    }
    ;

BatExpr:     mark (BatExpr, AtomExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);
        assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        mark (var ($1$->varname), var ($2$->varname))));
    }
    ;

BatExpr:     append (BatExpr, BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname); assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        /*
         * If we share our child with some other operator, we need
         * to copy it first.
         */
        if ($$->refctr == $1$->refctr)
            execute (assgn (var ($$->varname),
                            access (binsert (access (var ($1$->varname),
                                                     BAT_APPEND),
                                             var ($2$->varname)),
                                    BAT_READ)));
        else
            execute (assgn (var ($$->varname),
                            access (binsert (access (copy (var ($1$->varname)),
                                                     BAT_APPEND),
                                             var ($2$->varname)),
                                    BAT_READ)));
    }
    ;

AtomExpr:    oid (AtomExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        cast (type (m_oid), var ($1$->varname))));
    }
    ;

BatExpr:     moid (BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        mcast (type (m_oid), var ($1$->varname))));
    }
    ;

BatExpr:     mint (BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        mcast (type (m_int), var ($1$->varname))));
    }
    ;

BatExpr:     madd (BatExpr, AtomExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname); assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        madd (var ($1$->varname), var ($2$->varname))));
    }
    ;

BatExpr:     madd (AtomExpr, BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname); assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        madd (var ($1$->varname), var ($2$->varname))));
    }
    ;

BatExpr:     madd (BatExpr, BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname); assert ($2$ && $2$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname),
                        madd (var ($1$->varname), var ($2$->varname))));
    }
    ;

AtomExpr:    Literal;
AtomExpr:    count (BatExpr)
    =
    {
        if ($$->varname)
            break;

        assert ($1$ && $1$->varname);

        $$->varname = new_var ();

        execute (assgn (var ($$->varname), count (var ($1$->varname))));
    }
    ;

Literal:     lit_oid
    =
    {
        if ($$->varname)
            break;

        $$->varname = new_var ();

        execute (assgn (var ($$->varname), lit_oid ($$->sem.lit_val.val.o)));
    }
    ;

Literal:     lit_int
    =
    {
        if ($$->varname)
            break;

        $$->varname = new_var ();

        execute (assgn (var ($$->varname), lit_int ($$->sem.lit_val.val.i)));
    }
    ;

Literal:     lit_str
    =
    {
        if ($$->varname)
            break;

        $$->varname = new_var ();

        execute (assgn (var ($$->varname), lit_str ($$->sem.lit_val.val.s)));
    }
    ;

Literal:     lit_bit
    =
    {
        if ($$->varname)
            break;

        $$->varname = new_var ();

        execute (assgn (var ($$->varname), lit_bit ($$->sem.lit_val.val.b)));
    }
    ;

Literal:     lit_dbl
    =
    {
        if ($$->varname)
            break;

        $$->varname = new_var ();

        execute (assgn (var ($$->varname), lit_dbl ($$->sem.lit_val.val.d)));
    }
    ;

/* vim:set shiftwidth=4 expandtab filetype=c: */
