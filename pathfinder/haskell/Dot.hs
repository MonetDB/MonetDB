{-# OPTIONS -fallow-overlapping-instances #-}

{--

    Generate AT&T `dot' syntax to render algebra DAGs.

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

module Dot (dot) where

import Algb

import List (intersperse)
import Numeric (showInt)

----------------------------------------------------------------------
-- `dot' output

dot :: Algb -> String
dot q = (foldr1 (.)                    $ 
         intersperse (showString "\n") $
             ([showString "digraph XQuery {"]       ++ 
              map showString dot_header             ++ 
              map (do_dot (dag q)) (nodes (dag q))  ++ 
              [showChar '}']
             )
        ) "\n"
    where          
    dot_header :: [String] 
    dot_header = ["ordering=out;"
                 ,"node [shape=box];"
		 ,"node [height=0.2];"                              
		 ,"node [width=0.2];"                             
		 ,"node [style=filled];"
		 ,"node [color=grey];"                              
		 ,"node [fontsize=10];"
                 ]

    do_dot :: DAG AlgOp -> Node -> ShowS
    do_dot g n = 
        foldr1 (.)                 $
        intersperse (showChar ';') $
            ([showInt n . lbl]                                 ++
             map (\v -> showInt n       .
                        showString "->" . 
                        showInt v       .
                        showString ("[headlabel=\"" ++
                                    show v          ++
				    "\",labelfontcolor=grey]") 
		 ) (suc g n)                                   ++
             [showString ""]
            )
	where
        lbl :: ShowS
	lbl = case lab g n of
              Just op -> showString ("[" ++ label op ++ "]")
              Nothing -> error ("unlabelled node " ++ show n ++ " in graph")

        label :: AlgOp -> String
        label (ROWNUM n ps) =
            "label=\"\\\\tex[cc][cc]{$\\\\rho$~(" ++ sn ++ sp ps ++ ")}\",color=red,fontsize=6"
            where
            sn = case n of (a,as) -> a ++ ":(" ++ concat (intersperse "," as) ++ ")"
            sp []  = ""
            sp [a] = "/" ++ a
        label (PROJ cs) =
	    "label=\"\\\\tex[cc][cc]{$\\\\pi$~(" ++ 
             concat (intersperse "," (sp cs)) ++ ")}\",fontsize=7"
            where
            sp []                     = []
            sp ((n,a):ps) | n == a    = a:sp ps
                          | otherwise = (n ++ ":" ++ a):sp ps
        label (SEL c) =
	    "label=\"\\\\tex[cc][cc]{$\\\\sigma$~" ++ c ++ "}\",fontsize=5"
        label (TYPE c1 c2 t) =
            "label=\"\\\\tex[cc][cc]{TYPE " ++ c1 ++ ":" ++ c2 ++ "/" ++ 
             show t ++ "}\",fontsize=6"
        label (NEG c1 c2) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^{\\\\texttt{neg}}$ " ++ 
            c1 ++ ":" ++ c2 ++ "}\",fontsize=5"
        label (PLUS c cs) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^+$~" ++ 
	     c ++ ":(" ++
             concat (intersperse "," cs) ++ ")}\",fontsize=5"
        label (TIMES c cs) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^*$~" ++ 
	     c ++ ":(" ++
             concat (intersperse "," cs) ++ ")}\",fontsize=5"
        label (MINUS c cs) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^-$ " ++ 
	     c ++ ":(" ++
             concat (intersperse "," cs) ++ ")}\",fontsize=5"
        label (IDIV c cs) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^{\\\\texttt{idiv}}$ " ++ 
            c ++ ":(" ++ concat (intersperse "," cs) ++ ")}\",fontsize=5"
        label (DIV c cs) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^{\\\\texttt{div}}$ " ++ 
            c ++ ":(" ++ concat (intersperse "," cs) ++ ")}\",fontsize=5"
        label (MOD c cs) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^{\\\\texttt{mod}}$ " ++ 
            c ++ ":(" ++ concat (intersperse "," cs) ++ ")}\",fontsize=5"
        label (GRT c cs) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^{\\\\texttt{grt}}$ " ++ 
            c ++ ":(" ++ concat (intersperse "," cs) ++ ")}\",fontsize=5"
        label (EQL c cs) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^{\\\\texttt{eql}}$ " ++ 
            c ++ ":(" ++ concat (intersperse "," cs) ++ ")}\",fontsize=5"
        label (NOT c1 c2) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^{\\\\texttt{not}}$ " ++ 
            c1 ++ ":" ++ c2 ++ "}\",fontsize=5"
        label (OR c cs) =
	    "label=\"\\\\\tex[cc][cc]{$\\\\otimes^{\\\\texttt{or}}$ " ++ 
            c ++ ":(" ++ concat (intersperse "," cs) ++ ")}\",fontsize=5"
        label (AND c cs) =
	    "label=\"\\\\tex[cc][cc]{$\\\\otimes^{\\\\texttt{and}}$ " ++ 
            c ++ ":(" ++ concat (intersperse "," cs) ++ ")}\",fontsize=5"
        label (SUM c1 c2 ps) =
             "label=\"\\\\tex[cc][cc]{$\\\\bigotimes^{\\\\texttt{sum}}$ " ++ 
             c1 ++ ":(" ++ c2 ++ ")" ++ sp ps ++ "}\",fontsize=5"
	     where
	     sp [] = ""
             sp ps = "/" ++ concat (intersperse "," ps)
        label (COUNT c ps) =
             "label=\"\\\\tex[cc][cc]{$\\\\bigotimes^{\\\\texttt{count}}$ " ++ 
             c ++ sp ps ++ "}\",fontsize=5"
	     where
	     sp [] = ""
             sp ps = "/" ++ concat (intersperse "," ps)
        label (SEQTY1 c1 c2 ps) =
             "label=\"\\\\tex[cc][cc]{$\\\\bigotimes^{\\\\exists{}1}$ " ++ 
             c1 ++ ":(" ++ c2 ++ ")" ++ sp ps ++ "}\",fontsize=5"
	     where
	     sp [] = ""
             sp ps = "/" ++ concat (intersperse "," ps)
        label (ALL c1 c2 ps) =
             "label=\"\\\\tex[cc][cc]{$\\\\bigotimes^\\\\forall$ " ++ 
             c1 ++ ":(" ++ c2 ++ ")" ++ sp ps ++ "}\""
	     where
	     sp [] = ""
             sp ps = "/" ++ concat (intersperse "," ps)
        label U =
            "label=\"\\\\tex[cc][cc]{$\\\\cup$}\",fontsize=1"
        label DIFF =
	    "label=\"\\\\tex[cc][cc]{$\\\\setminus$}\",color=orange,fontsize=1"
        label DIST =
	    "label=\"\\\\tex[cc][cc]{$\\\\delta$}\",color=orange,fontsize=1"
	label X =
            "label=\"\\\\tex[cc][cc]{$\\\\times$}\",color=yellow,fontsize=1"
        label (JOIN (c1, c2)) =
	    "label=\"\\\\tex[cc][cc]{$\\\\Join$~(" ++
	     c1 ++ "=" ++ c2 ++ ")}\",color=green,fontsize=5"
        label (CINT c) =
	    "label=\"\\\\tex[cc][cc]{CINT " ++ c ++ "}\",fontsize=4" 
        label (CSTR c) =
	    "label=\"\\\\tex[cc][cc]{CSTR " ++ c ++ "}\",fontsize=4" 
        label (CDEC c) =
	    "label=\"\\\\tex[cc][cc]{CDEC " ++ c ++ "}\",fontsize=4" 
        label (CDBL c) =
	    "label=\"\\\\tex[cc][cc]{CDBL " ++ c ++ "}\",fontsize=4" 
        label ELEM =
	    "label=\"\\\\tex[cc][cc]{$\\\\varepsilon$}\",color=lawngreen,fontsize=1"
        label TEXT =
	    "label=\"\\\\tex[cc][cc]{$\\\\tau$}\",color=lawngreen,fontsize=1"
        label (SCJ (ax,kt,[])) =
            "label=\"\\\\tex[cc][cc]{\\\\scj{} " ++ 
            show ax ++ "::" ++ show kt ++ "()}\",color=lightblue,fontsize=6"
        label (SCJ (ax,_,[n])) =
            "label=\"\\\\tex[cc][cc]{\\\\scj{} " ++ 
            show ax ++ "::" ++ n ++ "}\",color=lightblue,fontsize=6"
        label (TBL ty ts) = 
	    "label=\"\\\\tex[cc][cc]{\\\\parbox{4cm}{\\\\centering TBL (" ++
            concat (intersperse "," (map (esc . fst)  ty)) ++ ")\\\\\\\\{}" ++
            concat (intersperse "," (map (esc . show) ts)) ++ "}}\",fontsize=2,height=0.4"
        label DMROOTS =
	    "label=\"\\\\tex[cc][cc]{DMROOTS}\",color=lightblue,fontsize=3"
        label DMFRAGS =
	    "label=\"\\\\tex[cc][cc]{DMFRAGS}\",color=lightblue,fontsize=3"
        label DMDOC =
	    "label=\"\\\\tex[cc][cc]{DMDOC}\",color=lightblue,fontsize=3"
        label DMDATA =
	    "label=\"\\\\tex[cc][cc]{DMDATA}\",color=lightblue,fontsize=3"
        label DMEMPTY =
	    "label=\"\\\\tex[cc][cc]{DMEMPTY}\",color=lightblue,fontsize=3"
        label DMU =
	    "label=\"\\\\tex[cc][cc]{$\\\\cup$}\",color=lightblue,fontsize=3"

        esc :: String -> String
        esc "" = ""
        esc ('"':cs) = "\\\"" ++ esc cs
	esc ('&':cs) = "\\\\&" ++ esc cs
	esc ('%':cs) = "\\\\%" ++ esc cs
	esc ('#':cs) = "\\\\#" ++ esc cs
        esc (c:cs)   = c:esc cs

                           


