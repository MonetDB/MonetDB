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

module Item (Item (..), unN, unI, unB) where

import Numeric (showFFloat)
import NumExts (floatToDouble, doubleToFloat)
----------------------------------------------------------------------
-- items
                                       -- value of type ...
data Item = I    Integer               --   int
          | S    String                --   str
          | B    Bool                  --   bool
          | D    Double                --   dbl
          | E    Float                 --   dec
          | O    Integer               --   nat (oid)
          | N    Integer               --   node (preorder rank)

instance Show Item where
    show (I x)     = show x
    show (S x)     = show x
    show (B True)  = "!1"
    show (B False) = "!0"
    show (D x)     = "#" ++ show x
    show (E x)     = "%" ++ showFFloat (Just 2) x ""
    show (O x)     = "&" ++ show x
    show (N x)     = "@" ++ show x

instance Num Item where
    (I x) + (I y) = I (x + y)
    (E x) + (I y) = E (x + fromInteger y)
    (I x) + (E y) = E (fromInteger x + y)
    (E x) + (E y) = E (x + y)
    (D x) + (I y) = D (x + fromInteger y)
    (I x) + (D y) = D (fromInteger x + y)
    (D x) + (E y) = D (x + floatToDouble y)
    (E x) + (D y) = D (floatToDouble x + y)
    (D x) + (D y) = D (x + y)

    (N x) + (I y) = N (x + y)        -- arithmetic with nodes:
    (I x) + (N y) = N (x + y)        -- used in pre/size plane axis predicates

    (I x) + (O y) = I (x + y)        -- arithmetic with nats (oids):
    (O x) + (I y) = I (x + y)        -- used in element construction

    (I x) * (I y) = I (x * y)

    negate (I x)  = I (negate x)
    abs (I x)     = I (abs x)
    signum (I x)  = I (signum x)

    fromInteger x = I x

instance Ord Item where
    compare (I x) (I y) = compare x y
    compare (S x) (S y) = compare x y
    compare (B x) (B y) = compare x y
    compare (E x) (E y) = compare x y
    compare (D x) (D y) = compare x y
    compare (O x) (O y) = compare x y
    compare (N x) (N y) = compare x y
    compare x     y     = error ("items cannot be ordered: " ++ 
                                 show x ++ ", " ++ show y)

instance Eq Item where
    (I x) == (I y) = x == y
    (S x) == (S y) = x == y
    (B x) == (B y) = x == y
    (E x) == (E y) = x == y
    (D x) == (D y) = x == y
    (O x) == (O y) = x == y
    (N x) == (N y) = x == y
    x     == y     = error ("items cannot be compared: " ++ 
                            show x ++ ", " ++ show y)

unN, unI :: Item -> Integer
unI (I x) = x
unN (N x) = x
unB (B x) = x