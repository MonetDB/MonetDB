{--

    Derives a DAG from an algebraic expression tree (includes
    functionality to emit AT&T `dot' output for generated DAGs).

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

module Dag (dag,dot) where

import Algb (Algb (..))

import List (intersperse, elemIndex)

------------------------------------------------------------------------
-- compute DAG from algebra expression tree

memo :: Algb -> [Algb] -> (Int, [Algb])
memo a ts = case elemIndex a ts of
	         Just i  -> (i,         ts)
		 Nothing -> (length ts, ts ++ [a])

-- dag a ts:
--  (1) turn arguments of a into DAG
--  (2) check if a (with arguments replaced by DAGs) is already memoized in ts
--  (3) return a's position in the list of memoized nodes ts and new ts
dag :: Algb -> [Algb] -> (Int, [Algb])
dag (ROWNUM n p c) ts = memo (ROWNUM n p (DAG n1)) ts1
    where
    (n1, ts1) = dag c ts

dag (PROJ p c) ts = memo (PROJ p (DAG n1)) ts1
    where
    (n1, ts1) = dag c ts

dag (SEL a c) ts = memo (SEL a (DAG n1)) ts1
    where
    (n1, ts1) = dag c ts

dag (TYPE a1 a2 t c) ts = memo (TYPE a1 a2 t (DAG (n1))) ts1
    where
    (n1, ts1) = dag c ts

dag (OP2 op t f a as c) ts = memo (OP2 op t f a as (DAG n1)) ts1
    where
    (n1, ts1) = dag c ts

dag (OP1 op t f a as c) ts = memo (OP1 op t f a as (DAG n1)) ts1
    where
    (n1, ts1) = dag c ts

dag (SUM a s p c) ts = memo (SUM a s p (DAG n1)) ts1
    where
    (n1, ts1) = dag c ts

dag (COUNT a p c) ts = memo (COUNT a p (DAG n1)) ts1
    where
    (n1, ts1) = dag c ts

dag (U c1 c2) ts = memo (U (DAG n1) (DAG n2)) ts2
    where
    (n1, ts1) = dag c1 ts 
    (n2, ts2) = dag c2 ts1 

dag (DIFF c1 c2) ts = memo (DIFF (DAG n1) (DAG n2)) ts2
    where
    (n1, ts1) = dag c1 ts 
    (n2, ts2) = dag c2 ts1 

dag (DIST c) ts = memo (DIST (DAG n1)) ts1
    where
    (n1, ts1) = dag c ts

dag (X c1 c2) ts = memo (X (DAG n1) (DAG n2)) ts2
    where
    (n1, ts1) = dag c1 ts 
    (n2, ts2) = dag c2 ts1 

dag (JOIN p c1 c2) ts = memo (JOIN p (DAG n1) (DAG n2)) ts2
    where
    (n1, ts1) = dag c1 ts 
    (n2, ts2) = dag c2 ts1 

dag (CINT a c) ts = memo (CINT a (DAG n1)) ts1
    where
    (n1, ts1) = dag c ts

dag (CSTR a c) ts = memo (CSTR a (DAG n1)) ts1
    where
    (n1, ts1) = dag c ts

dag (SCJ s c1 c2) ts = memo (SCJ s (DAG n1) (DAG n2)) ts2
    where
    (n1, ts1) = dag c1 ts 
    (n2, ts2) = dag c2 ts1 

dag (ELEM c1 c2 c3) ts = memo (ELEM (DAG n1) (DAG n2) (DAG n3)) ts3
     where
     (n1, ts1) = dag c1 ts 
     (n2, ts2) = dag c2 ts1 
     (n3, ts3) = dag c3 ts2 

dag (TEXT c1 c2) ts = memo (TEXT (DAG n1) (DAG n2)) ts2
    where
    (n1, ts1) = dag c1 ts 
    (n2, ts2) = dag c2 ts1 

dag a ts       = memo a ts

----------------------------------------------------------------------
-- generate `dot' input from algebra expression DAG

dot :: Algb -> String
dot a = (concat . intersperse "\n") (
	["digraph XQuery {",
         "ordering=out;",
         "node [shape=box];",       
         "node [height=0.2];",                              
         "node [width=0.2];",                               
         "node [style=filled];",                            
         "node [color=grey];",                              
         "node [fontsize=10];",
         "node [fontname=\"CMUTypewriter-Regular\"];"
--         "node [fontname=\"CMU Typewriter Text\"];"
        ]
        ++ map algbdot (zip [0..] d) 
        ++ ["}"])
    where
    (_, d) = dag a []

    algbdot :: (Int, Algb) -> String
    algbdot (d, ROWNUM n p (DAG n1)) =
	node d ("[label=\"\\\\tex[cc][cc]{$\\\\rho$~(" ++ sn ++ sp ++ ")}\",color=red, fontsize=6]") [n1]
	where
	sn = case n of (a,as) -> a ++ ":(" ++ concat (intersperse "," as) ++ ")"
        sp = case p of []  -> ""
                       [a] -> "/" ++ a

    algbdot (d, PROJ p (DAG n1)) =
	node d ("[label=\"\\\\tex[cc][cc]{$\\\\pi$~(" ++ concat (intersperse "," (sp p)) ++ ")}\", fontsize=7]") [n1]
	where
	sp []                     = []
	sp ((n,a):ps) | n == a    = a:sp ps
		      | otherwise = (n ++ ":" ++ a):sp ps

    algbdot (d, SEL a (DAG n1)) =
	node d ("[label=\"\\\\tex[cc][cc]{$\\\\sigma$~(" ++ a ++ ")}\"]") [n1]

    algbdot (d, TYPE a1 a2 t (DAG n1)) =
	node d ("[label=\"TYPE (" ++ a1 ++ "," ++ a2 ++ ")|\"]") [n1]

    algbdot (d, OP2 op _ _ a as (DAG n1)) =
	node d ("[label=\"" ++ op ++ " " ++ a ++ 
               ":(" ++ concat (intersperse "," as) ++ ")\"]") [n1]

    algbdot (d, OP1 op _ _ a as (DAG n1)) =
	node d ("[label=\"" ++ op ++ " " ++ a ++ 
               ":(" ++ as ++ ")\"]") [n1]

    algbdot (d, SUM a s p (DAG n1)) = 
        node d ("[label=\"SUM " ++ a ++ ":(" ++ s ++ ")" ++ sp ++ "\"]") [n1]
	where
        sp = case p of [] -> ""
                       as -> "/" ++ concat (intersperse "," as)

    algbdot (d, COUNT a p (DAG n1)) =
	node d ("[label=\"COUNT " ++ a ++ sp ++ "\"]") [n1] 
	where
        sp = case p of [] -> ""
                       as -> "/" ++ concat (intersperse "," as)

    algbdot (d, U (DAG n1) (DAG n2)) = 
	node d "[label=\"\\\\tex[cc][cc]{$\\\\cup$}\", fontsize=1]" [n1, n2]

    algbdot (d, DIFF (DAG n1) (DAG n2)) = 
	node d "[label=\"\\\\tex[cc][cc]{$\\\\setminus$}\",color=orange, fontsize=1]" [n1, n2]

    algbdot (d, DIST (DAG n1)) =
	node d "[label=\"\\\\tex[cc][cc]{$\\\\delta$}\",color=indianred, fontsize=1]" [n1] 

    algbdot (d, X (DAG n1) (DAG n2)) = 
	node d "[label=\"\\\\tex[cc][cc]{$\\\\times$}\",color=yellow,fontsize=1]" [n1, n2]

    algbdot (d, JOIN (a1,a2) (DAG n1) (DAG n2)) =
	node d ("[label=\"\\\\tex[cc][cc]{$\\\\Join$~(" ++ a1 ++ "=" ++ a2 ++ ")}\",color=green, fontsize=5]") [n1, n2]

    algbdot (d, CINT a (DAG n1)) = 
        node d ("[label=\"CINT (" ++ a ++ ")\"]") [n1]

    algbdot (d, CSTR a (DAG n1)) = 
        node d ("[label=\"CSTR (" ++ a ++ ")\"]") [n1]

    algbdot (d, CDEC a (DAG n1)) = 
        node d ("[label=\"CDEC (" ++ a ++ ")\"]") [n1]

    algbdot (d, CDBL a (DAG n1)) = 
        node d ("[label=\"CDBL (" ++ a ++ ")\"]") [n1]

    algbdot (d, SCJ (ax,kt,[]) (DAG n1) (DAG n2)) =
	node d ("[label=\"\\\\tex[cc][cc]{$\\\\scj$~" ++ show ax ++ show kt ++ "}\",color=lightblue]") [n1, n2] 

    algbdot (d, SCJ (ax,_,[n]) (DAG n1) (DAG n2)) =
	node d ("[label=\"\\\\tex[cc][cc]{$\\\\scj$~" ++ show ax ++ "::" ++ n ++ "}\",color=lightblue]") [n1, n2] 

    algbdot (d, ELEM (DAG n1) (DAG n2) (DAG n3)) =
	node d "[label=\"\\\\tex[cc][cc]{$\\\\varepsilon$}\",color=lawngreen, fontsize=1]" [n1, n2, n3]

    algbdot (d, TEXT (DAG n1) (DAG n2)) =
	node d "[label=\"\\\\tex[cc][cc]{$\\\\tau$}\",color=lawngreen, fontsize=1]" [n1, n2]

    algbdot (d, REL r) = 
	node d ("[label=\"REL " ++ r ++ "\"]") []
	 
    algbdot (d, TBL a t) =
        node d ("[label=\"TBL " ++ "(" ++ 
                concat (intersperse "," (map (escquot . fst) a)) ++ 
                ")\\n" ++
                concat (intersperse "," (map (escquot . show) t)) ++ "\"]") []

    node :: Int -> String -> [Int] -> String
    node d l cs = show d ++ l ++ 
                  (concat . intersperse ";" . 
                   map ((show d ++ " -> ") ++) . map show) cs ++ 
                  ";"

    escquot :: String -> String
    escquot "" = ""

    escquot ('"':cs) = "\\\"" ++ escquot cs
    escquot (c:cs)   = c:escquot cs

