{-# OPTIONS -fallow-overlapping-instances -fglasgow-exts #-}

{--

    Algebra evaluation (DAG-based).

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

    $Id$

--}

module CSE (cse, move) where

import Algb

import List (groupBy, sortBy, nubBy, (\\), nub)
import Control.Monad.State

----------------------------------------------------------------------
-- DAG CSE

cse :: (Eq a, Ord a) => (Node, DAG a) -> ((Node, DAG a), [(Node, Node)])
cse (r, g) = 
    ((move isos r, mkGraph ctxts)
    , 
     isos
    )
    where
    (ctxts, isos) = unify (contexts g, [])

    unify :: (Ord a) => ([Context a ()], [(Node, Node)]) -> 
                        ([Context a ()], [(Node, Node)])
    unify (cs, isos) | null isos' = (cs, isos)
                     | otherwise  = unify (cs', isos +->+ isos')
	where	
        (+->+) :: (Eq a) => [(a,a)] -> [(a,a)] -> [(a,a)]
        xs +->+ ys = 
	    nubBy (=<=) 
                  ([ (x1,y2) | (x1,x2) <- xs, (y1,y2) <- ys, x2 == y1 ] ++ 
                    xs                                                  ++ 
                    ys)
		where
		(=<=) :: (Eq a) => (a,b) -> (a,c) -> Bool
		(x,_) =<= (y,_) = x == y	

        isos' :: [(Node, Node)]
        isos' = concat $ map iso $ eqc $ cs
  
        cs' = 
          map (\c -> (node' c, lab' c, 
		      zip (repeat ()) (map (move isos') (suc' c)))) $
          reps                                                      $ 
	  eqc                                                       $ cs

        -- compute equivalence node equivalence classes based on
        -- node label (operator) and successors (arguments)
        eqc :: (Ord a) => [Context a ()] -> [[Context a ()]]
	eqc = groupBy grp_op . sortBy cmp_op
	    where
            oparg :: Context a () -> (a, [Node])
            oparg c = (lab' c, suc' c)            

            cmp_op :: Ord a => Context a () -> Context a () -> Ordering
            cmp_op x y = compare (oparg x) (oparg y)

            grp_op :: Eq a => Context a () -> Context a () -> Bool
            grp_op x y = (oparg x) == (oparg y)
	   
        -- representatives of equivalence classes
        reps :: [[Context a ()]] -> [Context a ()]
	reps = map head

        iso :: [Context a ()] -> [(Node, Node)]
	iso [c]    = []
	iso (c:cs) = zip (map node' cs) (repeat (node' c)) 


-- move x -> y according to isomorphism table isos 
-- (x -> x if x is not mentioned in isos)
move :: (Eq a) => [(a,a)] -> a -> a
move isos x = case (lookup x isos) of Just y -> y; Nothing -> x 

