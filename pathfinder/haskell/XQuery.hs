{-# OPTIONS -fallow-overlapping-instances #-}

{--

    XQuery playground (sample queries, tests, `main')

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

import DMGraph
import Item
import Algb
import Core
import Ty
import Compile
import CSE
import Dot 
import RelDot 
import Eval
import XMark

import List (intersperse, sortBy, mapAccumL, nub)
import Numeric (showInt, showFFloat)


-- compile XQuery Core expression, perform CSE, evaluate the
-- resulting DAG and serialize the final result
xquery :: Core -> String
xquery xq = 
    (foldr (.) id                 $ 
     intersperse (showString " ") $
     map (serial . head . item)   $ 
     sortBy cmp_pos               $ 
     extn r
    ) "\n"
    where
    -- compile XQuery, perform CSE on algebra DAG
    adag :: Algb
    isos :: [(Node,Node)]
    (adag, isos) = cse $ compile $ xq

    -- roots of sub-DAGs in CSE'd DAG which compute the live node fragments
    lvs :: [Node]
    lvs = nub $ map (move isos) $ live $ xq

    -- evaluate algebraic query, obtain relational result,
    -- relation/fragments DAG and next available XML node id (0)
    rdag :: DAG (RelFrag XMLTree)
    ((rdag, pre), res) = eval (empty, 0) adag 
    r = rel res

    -- evaluate the live node sub-DAGS
    -- to yield the actual XML live fragments
    -- (evaluation re-uses (partially) evaluated relation DAG rdag)
    fs :: [XMLTree]
    fs = concat                     $ 
         map frags                  $ 
         snd                        $ 
         mapAccumL eval (rdag, pre) $ (zip lvs (repeat (dag adag)))

    item, pos :: Tuple -> Tuple
    item = keys ["item"] (cols (schm r))
    pos  = keys ["pos"]  (cols (schm r))
	 
    cmp_pos :: Tuple -> Tuple -> Ordering
    cmp_pos x y = compare (pos x) (pos y)

    serial :: Item -> ShowS
    serial (I i)     = showString (show i)
    serial (S s)     = showString (show s)
    serial (B False) = showString "false"
    serial (B True)  = showString "true"
    serial (E e)     = showFFloat (Just 2) e
    serial (D d)     = showString (show d) 
    serial (N p)     = serialize xml p
	where
        -- find unique XML fragment containing node p
	xml = head $ filter (contains p) fs


-- compile XQuery Core expression, perform CSE, and emit
-- the resulting DAG in AT&T `dot' syntax
-- (run `make ps' to produce a PostScript file `XQuery.ps')
ps :: Core -> String
ps = dot . fst . cse . compile

-- compile XQuery Core expression, perform CSE, evaluate the
-- DAG yielding a DAG with relation/XML fragment anntotations,
-- and emit the resulting DAG in AT&T `dot' syntax
-- (run `make ps' to produce a PostScript file `XQuery.ps')
relps :: Core -> String
relps xq = reldot rfdag
    where
    (adag, isos)     = cse $ compile $ xq
    lvs              = nub $ map (move isos) $ live $ xq
    ((rdag, pre), _) = eval (empty :: DAG (RelFrag XMLTree), 0) adag
    ((rfdag, _), _)  = mapAccumL eval (rdag, pre) $ (zip lvs (repeat (dag adag)))
----------------------------------------------------------------------

xq = XFOR "u" (XSEQ (XINT 30) (XINT 20))
          (XFOR "v" (XSEQ (XINT 1) (XSEQ (XINT 2) (XINT 3)))
                (XIF (XEQ (XVAR "u") (XTIMES (XVAR "v") (XINT 10)))
                     (XSTR "match")
                     XEMPTY
                )
          )

xq' = XPATH (XELEM (XSTR "a") 
                   (XSEQ (XELEM (XSTR "b") (XTEXT (XSTR "foo")))
                         (XELEM (XSTR "b") (XTEXT (XSTR "bar")))
                   )
            )
            (Child, XMLElem, ["b"])
           

main = do print xmark_Q5
          putStr (xquery xmark_Q5)
          --putStr (ps xmark_Q5)
          --putStr (relps xmark_Q5)
	  

-- TODO:
--  . document CSE.cse
--  . add fn:root
--  . add missing XMark queries

-- Local Variables:
-- haskell-ghci-program-args: ("-package HaXml")
-- End:
