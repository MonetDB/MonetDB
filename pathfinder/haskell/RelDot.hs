{-# OPTIONS -fglasgow-exts #-}

{--

    Generate AT&T `dot' syntax to render relation DAGs.

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

module RelDot (reldot) where

import DM
import Item
import Algb
import Eval

import List (intersperse)
import Numeric (showInt)

-- in the relation DAGs, show (root node IDs, XML fragment) pairs
-- or show root node IDs only?
showXMLfragments :: Bool
showXMLfragments = True

----------------------------------------------------------------------
-- `dot' output

reldot :: (Show a, XML a Node) => DAG (RelFrag a) -> String
reldot d = (foldr1 (.)                    $ 
            intersperse (showString "\n") $
            ([showString "digraph XQuery {"] ++
             map showString dot_header       ++ 
             map (do_reldot d) (nodes d)     ++ 
             [showChar '}']
           )
           ) "\n"
    where          
    dot_header :: [String] 
    dot_header = ["ordering=out;"
                 ,"node [shape=record];"
		 ,"node [height=0.2];"                              
		 ,"node [width=0.2];"                             
        	 ,"node [style=filled];"
		 ,"node [color=black];"                              
		 ,"node [fillcolor=lightblue];"                              
		 ,"node [fontsize=8];"
                 ,"node [fontname=\"CMUTypewriter-Regular\"];"
                 ]

    do_reldot :: (Show a, XML a Node) => DAG (RelFrag a) -> Node -> ShowS
    do_reldot g n = 
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

        label :: (Show [a], Show a, XML a Node) => RelFrag a -> String
        label rf = "label=\"" ++ esc (render rf) ++ "\""

        -- render relation/XML fragment using dot's record label syntax
	render (Rel (R a t)) =
	    "{"                                                      ++ 
	    "{"  ++ concat (intersperse "|"   (map fst a))   ++ "}"  ++ 
            "|"                                                      ++
            "{{" ++ concat (intersperse "\\n" (map tuple t)) ++ "}}" ++
            "}"
        render (RelFrag fs r) 
	    = "{{" ++ render (Frag fs) ++ 
	      "|"                      ++ 
	              render (Rel r)   ++ "}}"
        render (Frag []) = 
	    "{\\\\tex[cc][cc]\\{$\\\\varnothing$\\}}"
	-- show root node IDs only or show (root node ID, fragment) pairs
        render (Frag fs) 
	    | showXMLfragments = 
		"{" ++ 
		concat (intersperse "|" (map show (zip (map (N . root) fs) fs))) ++ 
                "}"
            | otherwise       = 
		"{"                                                 ++ 
                concat (intersperse "|" (map (show . N . root) fs)) ++ 
                "}"

	tuple :: [Item] -> String
	tuple = concat . intersperse " " . map show

        esc :: String -> String
        esc "" = ""
        esc ('"':cs) = "\\\"" ++ esc cs
        esc ('<':cs) = "\\<" ++ esc cs
        esc ('>':cs) = "\\>" ++ esc cs
        esc (c:cs)   = c:esc cs

