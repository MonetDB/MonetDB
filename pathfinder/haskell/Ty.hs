{--

    Simple type system used during XQuery Core compilation.

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

module Ty (Ty (..), tyunion, tycommon) where

import List (nub)

----------------------------------------------------------------------
-- Types

data Ty = INT                              -- int
        | STR                              -- str
        | BOOL                             -- bool
        | DEC                              -- dec
        | DBL                              -- dbl
        | NAT                              -- nat (oid)
        | NODE                             -- node
	  deriving Eq

instance Show Ty where
    show INT     = "int"
    show STR     = "str"
    show BOOL    = "bool"
    show DEC     = "dec"
    show DBL     = "dbl"
    show NAT     = "nat"
    show NODE    = "node"

-- encodes the numeric type hierarchy
instance Ord Ty where
    compare INT INT = EQ
    compare INT DEC = LT
    compare INT DBL = LT
    compare DEC INT = GT
    compare DEC DEC = EQ
    compare DEC DBL = LT
    compare DBL INT = GT
    compare DBL DEC = GT
    compare DBL DBL = EQ
    compare t1  t2  = error ("no common numeric type for types: " ++ 
			     show t1 ++ ", " ++ show t2)

instance Show [Ty] where
    show []        = "ø"
    show [t]       = show t
    show (t:t':ts) = show t ++ "|" ++ show (t':ts)

tyunion :: ([Ty], [Ty]) -> [Ty]
tyunion (t1, t2) = nub (t1 ++ t2)

-- single common numeric type
tycommon :: [Ty] -> Ty
tycommon = maximum

