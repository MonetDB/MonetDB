{--

    Definition of relational algebra (target for XQuery Core compilation).

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

module Algb (Algb (..), 
             _PLUS, _TIMES, _MINUS, _DIV, _IDIV, _MOD, 
             _GT, _EQ, _NOT, _AND, _OR, _NEG,
             RelID, Attr, Op, Proj, Numb, Arg, Args, Part, Pred, Tuple,
             Rel (..), Type,
             schm, attrs, types, extn, idx, proj, keys,
             XPaxis (..), XPkind (..), XPname, XPstep) where

import Item (Item (..))
import Ty (Ty (..))

import List (intersperse, elemIndex, nub)

----------------------------------------------------------------------
-- Algebra


type RelID = String              -- relation name
type Attr  = String              -- attribute name
type Op    = String              -- operator name
type Proj  = (Attr, Attr)        -- pos:pos1
type Numb  = (Attr, [Attr])      -- pos:<ord,pos>
type Arg   = Attr                -- function argument/selection attribute
type Args  = [Arg]               -- function arguments
type Part  = [Attr]              -- optional row num/aggregation grouping key
type Pred  = (Attr, Attr)        -- equi-join predicate (pre = pre1)
type Tuple = [Item]              -- tuple of items

type Type = [(Attr, [Ty])]       -- relation schema

data Algb = ROWNUM Numb Part Algb
          | PROJ   [Proj] Algb
          | SEL    Arg  Algb
          | TYPE   Arg Arg Ty Algb
          | OP2    Op ([[Ty]] -> [Ty]) (Item -> Item -> Item) Arg Args Algb
          | OP1    Op ([[Ty]] -> [Ty]) (Item -> Item)         Arg Arg  Algb
          | SUM    Arg Arg Part Algb
          | COUNT  Arg Part Algb
          | SEQTY1 Arg Arg Part Algb
          | ALL    Arg Arg Part Algb
          | U      Algb Algb
          | DIFF   Algb Algb
          | DIST   Algb
	  | X      Algb Algb
	  | JOIN   Pred Algb Algb
          | CINT   Arg  Algb  
          | CSTR   Arg  Algb
          | CDEC   Arg  Algb
          | CDBL   Arg  Algb
          | ELEM   Algb Algb Algb
          | TEXT   Algb Algb 
	  | SCJ    XPstep Algb Algb
	  | REL    RelID
          | TBL    Type [Tuple]

          | DAG    Int

-- result type of arithmetic operators  
arithty :: [[Ty]] -> [Ty]
arithty ts = t
    where 
    t = case nub ts of
	[ty] -> ty
	tys  -> error ("arithmetic operator applied to arguments of type " ++
		       concat (intersperse "," (map show tys)))

_PLUS  = OP2 "+" arithty plus
	 where
	 (I x) `plus` (I y) = I (x + y)
	 (E x) `plus` (E y) = E (x + y)
	 (D x) `plus` (D y) = D (x + y)

_TIMES = OP2 "*" arithty times
	 where
	 (I x) `times` (I y) = I (x * y)
	 (E x) `times` (E y) = E (x * y)
	 (D x) `times` (D y) = D (x * y)
_MINUS = OP2 "-" arithty minus
	 where
	 (I x) `minus` (I y) = I (x - y)
	 (E x) `minus` (E y) = E (x - y)
	 (D x) `minus` (D y) = D (x - y)

_IDIV  = OP2 "IDIV" (const [INT]) idiv
	 where
	 (I x) `idiv` (I y) = I (truncate (fromInteger x / fromInteger y))
	 (E x) `idiv` (E y) = I (truncate (x / y))
	 (D x) `idiv` (D y) = I (truncate (x / y))

_DIV   = OP2 "IDIV" divty div
	 where
	 (I x) `div` (I y) = E (fromInteger x / fromInteger y)
	 (E x) `div` (E y) = E (x / y)
	 (D x) `div` (D y) = D (x / y)

         divty [[INT],[INT]] = [DEC]
         divty [[DEC],[DEC]] = [DEC]
         divty [[DBL],[DBL]] = [DBL]

_MOD   = OP2 "MOD" (const [INT]) modulo
	 where
	 (I x) `modulo` (I y) = I (mod x y)

_GT    = OP2 ">" (const [BOOL]) gt                       
         where
	 (I x) `gt` (I y) = B (x > y)
	 (S x) `gt` (S y) = B (x > y)
	 (B x) `gt` (B y) = B (x > y)
	 (E x) `gt` (E y) = B (x > y)
	 (D x) `gt` (D y) = B (x > y)
	 (N x) `gt` (N y) = B (x > y)

_EQ    = OP2 "=" (const [BOOL]) eq                       
	 where
	 (I x) `eq` (I y) = B (x == y)
	 (S x) `eq` (S y) = B (x == y)
	 (B x) `eq` (B y) = B (x == y)
	 (E x) `eq` (E y) = B (x == y)
	 (D x) `eq` (D y) = B (x == y)
	 (N x) `eq` (N y) = B (x == y)

_NOT   = OP1 "NOT" (const [BOOL]) _not
	 where
	 _not (B x) = B (not x)

_AND   = OP2 "AND" (const [BOOL]) and                   
         where
         (B x) `and` (B y) = B (x && y)

_OR    = OP2 "OR" (const [BOOL]) or                     
         where
         (B x) `or` (B y)  = B (x || y)

_NEG   = OP1 "NEG" arithty neg
	 where
	 neg (I x) = I (negate x)
	 neg (E x) = E (negate x)
	 neg (D x) = D (negate x)

instance Show Algb where
    show = s
	where
	s (ROWNUM n p c)         = "[ROW# (" ++ sn ++ sp ++ ")" ++ 
                                   s c ++ "]"
	    where
	    sn = case n of (a,as) -> a ++ ":(" ++ concat (intersperse "," as) ++ ")"
	    sp = case p of [] -> ""
			   as -> "/" ++ concat (intersperse "," as)

        s (PROJ p c)             = "[¶ (" ++ 
                                   concat (intersperse "," (sp p)) ++ ")" ++
                                   s c ++ "]"
	    where
	    sp []         = []
	    sp ((n,a):ps) = (n ++ ":" ++ a):sp ps
        s (SEL a c)              = "[SEL (" ++ a ++ ")" ++ s c ++ "]"
        s (TYPE a1 a2 t c)       = "[TYPE " ++ a1 ++ ":(" ++ a2 ++ ")/" ++
                                   show t ++ s c ++ "]"
        s (OP2 op _ _ a as c)    = "[" ++ op ++ " " ++ a ++ ":(" ++ 
                                   concat (intersperse "," as) ++  
                                   ")" ++ s c ++ "]"	 
        s (OP1 op _ _ a as c)    = "[" ++ op ++ " " ++ a ++ 
                                   ":(" ++ as ++ ")" ++ 
                                   s c ++ "]"
        s (SUM a s' p c)         = "[SUM " ++ a ++ ":(" ++ s' ++ ")" ++ sp ++ 
                                   s c ++ "]"
	    where
	    sp = case p of [] -> ""
			   as -> "/" ++ concat (intersperse "," as)
        s (COUNT a p c)          = "[COUNT " ++ a ++ sp ++ s c ++ "]"
	    where
	    sp = case p of [] -> ""
			   as -> "/" ++ concat (intersperse "," as)
        s (SEQTY1 a s' p c)      = "[SEQTY1 " ++ a ++ ":(" ++ s' ++ ")" ++ 
                                   sp ++ s c ++ "]"
	    where
	    sp = case p of [] -> ""
			   as -> "/" ++ concat (intersperse "," as)
        s (ALL a s' p c)         = "[ALL " ++ a ++ ":(" ++ s' ++ ")" ++ sp ++ 
                                   s c ++ "]"
	    where
	    sp = case p of [] -> ""
			   as -> "/" ++ concat (intersperse "," as)
        s (U c1 c2)              = "[U" ++ s c1 ++ s c2 ++ "]"
        s (DIFF c1 c2)           = "[\\\\" ++ s c1 ++ s c2 ++ "]"
        s (DIST c)               = "[DIST " ++ s c ++ "]"
        s (X c1 c2)              = "[×" ++ s c1 ++ s c2 ++ "]"
        s (JOIN (a1,a2) c1 c2)   = "[|X| (" ++ a1 ++ "=" ++ a2 ++ ")" ++ 
                                   s c1 ++ s c2 ++ "]"
        s (CINT a c)             = "[CINT (" ++ a ++ ")" ++ s c ++ "]"
        s (CSTR a c)             = "[CSTR (" ++ a ++ ")" ++ s c ++ "]"
        s (CDEC a c)             = "[CDEC (" ++ a ++ ")" ++ s c ++ "]"
        s (CDBL a c)             = "[CDBL (" ++ a ++ ")" ++ s c ++ "]"
        s (SCJ (ax,kt,[]) c1 c2) = "[/|" ++ show ax ++ show kt ++
                                    s c1 ++ s c2 ++ "]" 
        s (SCJ (ax,_,[n]) c1 c2) = "[/|" ++ show ax ++ "::" ++ n ++ 
                                    s c1 ++ s c2 ++ "]" 
        s (ELEM c1 c2 c3)        = "[ELEM" ++ s c1 ++ s c2 ++ s c3 ++ "]"
        s (TEXT c1 c2)           = "[TEXT" ++ s c1 ++ s c2 ++ "]"
        s (REL r)                = "[REL " ++ r ++ "]"
        s (TBL a t)              = "[TBL " ++ "(" ++ 
                                   concat (intersperse "," (map attr a)) ++ 
                                   ")" ++
		  	           concat (intersperse "," (map show t)) ++ 
                                   "]"
	    where
	    attr (a,ty) = "(" ++ a ++ "," ++ show ty ++ ")"
        s (DAG i)                = "[DAG (" ++ show i ++ ")]"


-- equality for two algebraic expressions (to construct DAG from
-- algebraic expression tree) 
--
-- NB. since ELEM/TEXT have side effects (node construction), they
--     are considered _not_ equal here
instance Eq Algb where
    (ROWNUM n p c)      == (ROWNUM n' p' c')        = (n,p,c) == (n',p',c')
    (PROJ p c)          == (PROJ p' c')             = (p,c) == (p',c')
    (SEL a c)           == (SEL a' c')              = (a,c) == (a',c')
    (TYPE a1 a2 t c)    == (TYPE a1' a2' t' c')     = 
        (a1,a2,t,c) == (a1',a2',t',c')
    (OP2 op _ _ a as c) == (OP2 op' _ _ a' as' c')  = 
	(op,a,as,c) == (op',a',as',c')
    (OP1 op _ _ a as c) == (OP1 op' _ _ a' as' c')  =
	(op,a,as,c) == (op',a',as',c')
    (SUM a s p c)       == (SUM a' s' p' c')        = 
        (a,s,p,c) == (a',s',p',c')
    (SEQTY1 a s p c)    == (SEQTY1 a' s' p' c')     = 
        (a,s,p,c) == (a',s',p',c')
    (ALL a s p c)       == (ALL a' s' p' c')        = 
        (a,s,p,c) == (a',s',p',c')
    (COUNT a p c)       == (COUNT a' p' c')         = (a,p,c) == (a',p',c')
    (U c1 c2)           == (U c1' c2')              = (c1,c2) == (c1',c2')
    (DIFF c1 c2)        == (DIFF c1' c2')           = (c1,c2) == (c1',c2')
    (DIST c)            == (DIST c')                = c == c'
    (X c1 c2)           == (X c1' c2')              = (c1,c2) == (c1',c2')
    (JOIN p c1 c2)      == (JOIN p' c1' c2')        = (p,c1,c2) == (p',c1',c2')
    (CINT a c)          == (CINT a' c')             = (a,c) == (a',c')
    (CSTR a c)          == (CSTR a' c')             = (a,c) == (a',c')
    (CDEC a c)          == (CDEC a' c')             = (a,c) == (a',c')
    (CDBL a c)          == (CDBL a' c')             = (a,c) == (a',c')
    (SCJ s c1 c2)       == (SCJ s' c1' c2')         = (s,c1,c2) == (s',c1',c2')
    (ELEM _ _ _)        == (ELEM _ _ _)             = False
    (TEXT _ _)          == (TEXT _ _)               = False
    (REL r)             == (REL r')                 = r == r'
    (TBL a t)           == (TBL a' t')              = (a,t) == (a',t')
    (DAG i)             == (DAG i')                 = i == i'
    _                   == _                        = False

----------------------------------------------------------------------
-- Relations (schema, extension)

data Rel = R Type [Tuple]
	 deriving Show

-- relation schema
schm :: Rel -> Type
schm (R a _) = a

-- attribute names
attrs :: Type -> [Attr]
attrs = map fst

-- attribute types
types :: Type -> [[Ty]]
types = map snd

-- relation extension (= tuples)
extn :: Rel -> [Tuple]
extn (R _ t) = t

-- attribute index
idx :: Attr -> [Attr] -> Int
idx a as = case elemIndex a as of
                Just i  -> i
                Nothing -> error ("no attribute " ++ a ++ " in " ++ show as)

-- translate projection list into attribute index list
proj :: [Attr] -> [Attr] -> [Int]
proj p as = map (\a -> idx a as) p


-- create function to extract key attributes k (schema as) from a tuple
keys :: [Attr] -> [Attr] -> ([a] -> [a])
keys k as = pam (map (flip (!!)) (proj k as))
    where
    pam :: [a -> b] -> a -> [b]
    pam []     _ = []
    pam (f:fs) x = f x : pam fs x

-- XPath sublanguage
data XPaxis = Descendant                 -- XPath axes
            | Descendant_or_self
	    | Ancestor
	    | Following
	    | Preceding
            | Child
              deriving Eq
data XPkind = Elem                       -- XPath kind test
            | Text
            | Node
              deriving Eq
type XPname = String                     -- XPath name test
type XPstep = (XPaxis, XPkind, [XPname]) -- XPath axis step

instance Show XPaxis where
    show Descendant         = "descendant"
    show Descendant_or_self = "descendant-or-self"
    show Ancestor           = "ancestor"
    show Following          = "following"
    show Preceding          = "preceding"
    show Child              = "child"

instance Show XPkind where
    show Elem = "::*"
    show Text = "::text()"
    show Node = "::node()"

