{-# OPTIONS -fallow-overlapping-instances #-}

{--

    Definition of relational algebra DAGs and relations
    (target for XQuery Core compilation).

    Copyright Notice:
    -----------------

     The contents of this file are subject to the MonetDB Public
     License Version 1.0 (the "License"); you may not use this file
     except in compliance with the License. You may obtain a copy of
     the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html

     Software distributed under the License is distributed on an "AS
     IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
     implied. See the License for the specific language governing
     rights and limitations under the License.

     The Original Code is the ``Pathfinder'' system. The Initial
     Developer of the Original Code is the Database & Information
     Systems Group at the University of Konstanz, Germany. Portions
     created by U Konstanz are Copyright (C) 2000-2004 University
     of Konstanz. All Rights Reserved.

     Contributors:
             Torsten Grust <torsten.grust@uni-konstanz.de>
             Jens Teubner <jens.teubner@uni-konstanz.de>
             Sabine Mayer <sabine.mayer@uni-konstanz.de>

    $Id$

--}

module Algb (Algb, AlgOp (..), 
             Frags,
             algb, dag, top,
             Col, Numb, Part, Type, Tuple, 
             Rel (..), schm, cols, types, extn, idx, proj, keys,
             DAG,
             module GraphFM) where

import Core
import Ty
import Item
import GraphFM

import Control.Monad.State
import List (elemIndex)


data AlgOp = ROWNUM Numb Part
           | PROJ   [Proj]
           | SEL    Col
           | TYPE   Col Col Ty
           | NEG    Col Col
           | PLUS   Col Cols
           | TIMES  Col Cols
           | MINUS  Col Cols
           | IDIV   Col Cols
           | DIV    Col Cols
           | MOD    Col Cols
           | GRT    Col Cols
           | EQL    Col Cols
           | NOT    Col Col
           | OR     Col Cols
           | AND    Col Cols
           | SUM    Col Col Part
           | COUNT  Col Part
           | SEQTY1 Col Col Part
           | ALL    Col Col Part 
           | U
           | DIFF
           | DIST
           | X
           | JOIN   (Col, Col)
           | CINT   Col
           | CSTR   Col
           | CDEC   Col
           | CDBL   Col
           | ELEM
           | TEXT
           | SCJ    XPstep
           | TBL    Type [Tuple]
           | DMROOTS
           | DMFRAGS
           | DMDOC
           | DMDATA
           | DMEMPTY
           | DMU
	     deriving (Ord,Show)

-- equality for algebraic operators used during CSE
-- (NB. algebraic operators w/ side effects (ELEM, TEXT) 
-- are _not_ equal to their kind)

instance Eq AlgOp where
    ROWNUM n ps    == ROWNUM n' ps'     = (n,ps) == (n',ps')
    PROJ cs        == PROJ cs'          = cs == cs'
    SEL c          == SEL c'            = c == c'    
    TYPE c1 c2 t   == TYPE c1' c2' t'   = (c1,c2,t) == (c1',c2',t')
    NEG c1 c2      == NEG c1' c2'       = (c1,c2) == (c1',c2')
    PLUS c cs      == PLUS c' cs'       = (c,cs) == (c',cs')
    TIMES c cs     == TIMES c' cs'      = (c,cs) == (c',cs')
    MINUS c cs     == MINUS c' cs'      = (c,cs) == (c',cs')
    IDIV c cs      == IDIV c' cs'       = (c,cs) == (c',cs')
    DIV c cs       == DIV c' cs'        = (c,cs) == (c',cs')
    MOD c cs       == MOD c' cs'        = (c,cs) == (c',cs')
    GRT c cs       == GRT c' cs'        = (c,cs) == (c',cs')
    EQL c cs       == EQL c' cs'        = (c,cs) == (c',cs')
    NOT c1 c2      == NOT c1' c2'       = (c1,c2) == (c1',c2')
    OR c cs        == OR c' cs'         = (c,cs) == (c',cs')
    AND c cs       == AND c' cs'        = (c,cs) == (c',cs')
    SUM c1 c2 ps   == SUM c1' c2' ps'   = (c1,c2,ps) == (c1',c2',ps') 
    COUNT c ps     == COUNT c' ps'      = (c,ps) == (c',ps') 
    SEQTY1 c1 c2 p == SEQTY1 c1' c2' p' = (c1,c2,p) == (c1',c2',p')
    ALL c1 c2 p    == ALL c1' c2' p'    = (c1,c2,p) == (c1',c2',p')
    U              == U                 = True
    DIFF           == DIFF              = True
    DIST           == DIST              = True
    X              == X                 = True
    JOIN p         == JOIN p'           = p == p'
    CINT c         == CINT c'           = c == c'
    CSTR c         == CSTR c'           = c == c'
    CDEC c         == CDEC c'           = c == c'
    CDBL c         == CDBL c'           = c == c'
    ELEM           == ELEM              = False                        -- (!)
    TEXT           == TEXT              = False                        -- (!)
    SCJ s          == SCJ s'            = s == s'
    TBL a ts       == TBL a' ts'        = (a,ts) == (a',ts')
    DMROOTS        == DMROOTS           = True
    DMFRAGS        == DMFRAGS           = True
    DMDOC          == DMDOC             = True
    DMDATA         == DMDATA            = True
    DMEMPTY        == DMEMPTY           = True
    DMU            == DMU               = True
    _              == _                 = False

type Col   = String                 -- column name
type Cols  = [Col]                  -- column names
type Tuple = [Item]                 -- tuple of items
type Proj  = (Col, Col)             -- item:pre
type Numb  = (Col, [Col])           -- pos:<ord,pos>
type Part  = [Col]                  -- optional rownum/aggregation grouping key

-- table/relation type
type Type  = [(Col, [Ty])]

-- a DAG
type DAG a = Gr a ()

-- a relational algebra expression: a DAG of algebra operators and
-- the DAG's top (root) node
type Algb  = (Node, DAG AlgOp)

-- live fragments of a queryL a DAG of algebra operators and
-- the list of roots of the sub-DAGs computing those fragments
type Frags = ([Node], DAG AlgOp)

dag :: Algb -> DAG AlgOp
top :: Algb -> Node
dag = snd
top = fst

-- monadic algebra DAG construction
algb :: State (DAG AlgOp) Node -> Algb
algb a = runState a empty


----------------------------------------------------------------------
-- relations

data Rel = R Type [Tuple]
	   deriving Show

-- relation schema
schm :: Rel -> Type
schm (R a _) = a

-- column names
cols :: Type -> [Col]
cols = map fst

-- column types
types :: Type -> [[Ty]]
types = map snd

-- relation extension (= tuples)
extn :: Rel -> [Tuple]
extn (R _ t) = t

-- column index
idx :: Col -> [Col] -> Int
idx a as = case elemIndex a as of
                Just i  -> i
                Nothing -> error ("no attribute " ++ a ++ " in " ++ show as)

-- translate projection list into column index list
proj :: [Col] -> [Col] -> [Int]
proj p as = map (\a -> idx a as) p

-- create function to extract key columns k (schema as) from a tuple
keys :: [Col] -> [Col] -> ([a] -> [a])
keys k as = pam (map (flip (!!)) (proj k as))
    where
    pam :: [a -> b] -> a -> [b]
    pam []     _ = []
    pam (f:fs) x = f x : pam fs x




