{-# OPTIONS -fallow-overlapping-instances -fglasgow-exts -fno-cse #-}

{--

    Implementation of the XML Data Model based on FGL 
    (functional graphs).

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

module DMGraph (XMLTree,
	        module DM) where

import DM
import GraphFM

import Text.XML.HaXml.Parse 
import Text.XML.HaXml.Types
import Foreign (unsafePerformIO)

----------------------------------------------------------------------
-- XML trees based on FGL

data XMLNode = XDoc String            -- document node (URI)
             | XElem String           -- element node (tag)
             | XText String           -- text node (CDATA)
	       deriving (Eq, Show)

type XMLTree = Gr XMLNode ()

instance XML (Gr XMLNode ()) Node where
    root xml = case (lookup 0 num_parents) of
		    Just r  -> r
                    Nothing -> error ("no root in XML tree")
	where
        num_parents :: [(Int, Node)]
	num_parents = zip (map (length . pre xml) (nodes xml)) (nodes xml)

    string_value xml n = 
        (concat . map (sv . context xml)) (descendant_or_self xml n)
	where
	sv :: Context XMLNode () -> String
        sv (_, XText s, _) = s
        sv _               = ""

    parent xml n = pre xml n

    child xml n = suc xml n

    descendant xml n = (concat . map desc) (child xml n)
	where
        desc :: Int -> [Int]
	desc v = v:(concat . map desc) (child xml v)

    subtree = subgraph

    kind xml n = 
	case (context xml n) of
        (_, XDoc _, _)  -> XMLDoc			     
        (_, XElem _, _) -> XMLElem
        (_, XText _, _) -> XMLText

    name xml n =
        case (context xml n) of
        (_ , XElem tag, _) -> tag
        _                  -> ""

    text n s = (n + 1, (n, XText s, []) & empty)

    element n (t, xmls) =
        (last new_roots, new_tree)
        where
	subtree_sizes :: [Int]
        subtree_sizes = map (length . nodes) xmls

        new_roots :: [Int]
        new_roots = scanl (+) (n + 1) subtree_sizes

        new_subtrees :: [XMLTree]
        new_subtrees = zipWith shift (init new_roots) xmls

        new_tree :: XMLTree
        new_tree = (n, XElem t, zip (repeat ()) (init new_roots))
                   &
                   mkGraph (concat (map contexts new_subtrees))

    document n uri xml = 
        (n + size + 1, new_tree)
        where
        size :: Int
        size = length (nodes xml)

	subtree :: XMLTree
        subtree = shift (n + 1) xml

        new_tree :: XMLTree
        new_tree = (n, XDoc uri, [((), root subtree)])
                   &
                   subtree

    serialize xml n =
	serial (context xml n)
	where
        serial :: Context XMLNode () -> ShowS
        serial (_, XDoc _, [child]) =
            serial (context xml (snd child))
        serial (_, XElem t, []) =
            showChar '<' . showString t . showString "/>"
        serial (_, XElem t, children) =
	    showChar '<' . showString t . showChar '>' .
            foldr (.) id (map (serial . context xml . snd) children) .
	    showString "</" . showString t . showChar '>'
	serial (_, XText s, []) = 
	    showString s

    {-# NOINLINE doc #-}

    -- load a document from an XML file (uses the HaXML parser)
    doc n uri = unsafePerformIO $
	        do fc <- readFile uri
	           let (Document _ _ r) = xmlParse uri fc 
	           return (document n uri (haxe r))
        where
	haxe :: Element -> Gr XMLNode ()
        haxe (Elem t _ cs) = snd (element 0 (t, map haxc cs))

        haxc :: Content -> Gr XMLNode ()
	haxc (CString _ s) = snd (text 0 s)
        haxc (CElem e)     = haxe e

		
-- perform ``node renumbering''       
shift :: Int -> XMLTree -> XMLTree
shift s xml = mkGraph $ map shift' $ contexts $ xml
    where
    offs = s - root xml

    shift' :: Context XMLNode () -> Context XMLNode ()
    shift' (v,l,es) = (v + offs, l, zip (repeat ()) (map ((+ offs) . snd) es))


instance Show (Gr XMLNode ()) where
    show xml = serialize xml (root xml) ""
