{-# OPTIONS -fallow-overlapping-instances #-}
{--

    Definition of XQuery items.

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

module Item (Item (..), unN, unI, unB, unS, unO) where

import Graph (Node)

import Numeric (showFFloat)
import NumExts (floatToDouble, doubleToFloat)


-- XQuery items
                                       -- value of type ...
data Item = I    Integer               --   int
          | S    String                --   str
          | B    Bool                  --   bool
          | D    Double                --   dbl
          | E    Float                 --   dec
          | O    Int                   --   nat (oid)
          | N    Node                  --   node (preorder rank)
	    deriving (Eq, Ord)

instance Show Item where
    show (I x)     = show x
    show (S x)     = show x
    show (B True)  = "!1"
    show (B False) = "!0"
    show (D x)     = "#" ++ show x
    show (E x)     = "%" ++ showFFloat (Just 2) x ""
    show (O x)     = "&" ++ show x
    show (N x)     = "@" ++ show x


unI :: Item -> Integer
unI (I x) = x
unN :: Item -> Int
unN (N x) = x
unB :: Item -> Bool
unB (B x) = x
unS :: Item -> String
unS (S x) = x
unO :: Item -> Int
unO (O x) = x