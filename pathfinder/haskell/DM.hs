{-# OPTIONS -fglasgow-exts #-}
{--

    Interface to the XML Data Model.

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


module DM (XML (..), XMLid, XMLkind (..),
           QName) where

----------------------------------------------------------------------
-- XML Data Model


-- XML QName

type QName = String


-- XML node kinds

data XMLkind = XMLNode
             | XMLDoc
             | XMLElem
             | XMLText
	       deriving (Ord, Eq)

instance Show XMLkind where
    show XMLNode = "node"
    show XMLDoc  = "document-node"
    show XMLElem = "element"
    show XMLText = "text"


-- XML node identifiers
-- (also reflects document order)
type XMLid = Int

-- XML instances 
-- (XML nodes carry comparable and orderable identifiers of type XMLid)

class (Integral n) => XML t n | t -> n where
    -- id of document node/element root node if given XML tree
    root          :: t -> n

    -- string value of the identified node
    string_value  :: t -> n -> String

    -- nodes in the child axis of the identified
    -- context node (in doc.order)
    child         :: t -> n -> [n]

    -- node in parent axis of the identified context node
    -- (may return [] if context node is the document node/
    -- the root element node)
    parent        :: t -> n -> [n]

    -- nodes in the descendant axis of the identified
    -- context node (in doc.order)
    descendant    :: t -> n -> [n]

    -- nodes in the descendant-or-self axis of the identified
    -- context node (in doc.order)
    descendant_or_self :: t -> n -> [n]
    descendant_or_self t n = n:descendant t n

    -- does the given tree contain the identified node?
    contains :: n -> t -> Bool
    contains n t = elem n (descendant_or_self t (root t))

    -- extract the subtree rooted in the identified node
    subtree       :: t -> n -> t

    -- construct XML document node with given node id,
    -- document URI, and XML subtree, returns 
    -- (next available node id, XML document tree)		     
    document      :: n -> String -> t -> (n, t)

    -- construct XML element node with given node id, 
    -- tag name, and ordered list of content XML subtrees, 
    -- returns (next available node id, XML tree)
    element       :: n -> (QName, [t]) -> (n, t)

    -- construct XML text node with given node if and
    -- character data context, 
    -- returns (next available node id, XML tree)
    text          :: n -> String -> (n, t)

    -- serialize the identified node (and its subtree) 
    serialize     :: t -> n -> ShowS

    -- load an XML document from the given file, 
    -- return (next available node id, XML document tree)
    doc           :: n -> String -> (n, t)

    -- kind of idenfitied node in XML tree
    kind          :: t -> n -> XMLkind

    -- name of idenfitied node in XML tree
    name          :: t -> n -> QName
    

