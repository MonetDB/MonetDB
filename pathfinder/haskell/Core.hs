{-# OPTIONS -fallow-overlapping-instances #-}

{--
    Definition of source language XQuery Core.

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

module Core (Core (..),
             XPaxis (..), XPstep, SeqTy (..)) where

import Ty 
import DM

import List (intersperse)
import Numeric (showFFloat)


-- XQuery Core dialect

data Core = XINT     Integer
          | XSTR     String                    -- "foo"
          | XDBL     Double                    -- 4.2e1
          | XDEC     Float                     -- 4.20
          | XEMPTY                             -- ()
	  | XVAR     QName                     -- $x
          | XPLUS    Core Core                 -- e1 + e2
          | XTIMES   Core Core                 -- e1 * e2
          | XMINUS   Core Core                 -- e1 - e2
          | XDIV     Core Core                 -- e1 div e2
          | XIDIV    Core Core                 -- e1 idiv e2
          | XMOD     Core Core                 -- e1 mod e2
          | XLT      Core Core                 -- e1 < e2
          | XGT      Core Core                 -- e1 > e2
          | XEQ      Core Core                 -- e1 = e2
          | XOR      Core Core                 -- e1 and e2
          | XAND     Core Core                 -- e1 or e2
          | XNEG     Core                      -- - e
          | XIS      Core Core                 -- e1 is e2
          | XBEFORE  Core Core                 -- e1 << e2
          | XAFTER   Core Core                 -- e1 >> e2
          | XFNNOT   Core                      -- fn:not (e)
          | XFNSUM   Core                      -- fn:sum (e)
          | XFNCOUNT Core                      -- fn:count (e)
          | XFNEMPTY Core                      -- fn:empty (e)
          | XFNDIST  Core                      -- fn:distinct-values (e)
          | XFNDDO   Core                      -- fn:distinct-doc-order (e)
          | XFNDOC   Core                      -- fn:doc (e)
          | XFNDATA  Core                      -- fn:data (e)
          | XFNTRUE                            -- fn:true ()
          | XFNFALSE                           -- fn:false ()
          | XFNROOT  Core                      -- fn:root (e)
          | XCASTINT Core                      -- xs:integer (e)
          | XCASTSTR Core                      -- xs:string (e)
          | XCASTDEC Core                      -- xs:decimal (e)
          | XCASTDBL Core                      -- xs:double (e)
          | XTYPESW  Core SeqTy Core Core      -- typeswitch (e1)
                                               --    case t  return e2
                                               --    default return e3
	  | XLET     QName Core Core           -- let $x := e1 return e2
	  | XFOR     QName Core Core           -- for $x in e1 return e2
	  | XFORAT   QName QName Core Core     -- for $x at $p in e1 return e2
          | XORDER   Core [Core]               -- e1 order by e2, e3, ..., en
          | XIF      Core Core Core            -- if e1 then e2 else e3
	  | XSEQ     Core Core                 -- e1, e2
	  | XFUN     QName [Core]              -- f (e1, e2, ..., en)
          | XELEM    Core Core                 -- element {e1} {e2}
          | XTEXT    Core                      -- text {e}
          | XPATH    Core XPstep               -- e / ax::n


instance Show Core where
    show (XINT i)              = show i
    show (XDBL d)              = show d
    show (XDEC e)              = showFFloat (Just 2) e ""
    show (XSTR s)              = show s
    show XEMPTY                = "()"
    show (XVAR v)              = "$" ++ v
    show (XPLUS e1 e2)         = show e1 ++ " + " ++ show e2
    show (XTIMES e1 e2)        = show e1 ++ " * " ++ show e2
    show (XMINUS e1 e2)        = show e1 ++ " - " ++ show e2
    show (XDIV e1 e2)          = show e1 ++ " div " ++ show e2
    show (XIDIV e1 e2)         = show e1 ++ " idiv " ++ show e2
    show (XMOD e1 e2)          = show e1 ++ " mod " ++ show e2
    show (XLT e1 e2)           = show e1 ++ " < " ++ show e2
    show (XGT e1 e2)           = show e1 ++ " > " ++ show e2
    show (XEQ e1 e2)           = show e1 ++ " = " ++ show e2
    show (XOR e1 e2)           = show e1 ++ " or " ++ show e2
    show (XAND e1 e2)          = show e1 ++ " and " ++ show e2
    show (XNEG e)              = "-" ++ show e
    show (XIS e1 e2)           = show e1 ++ " is " ++ show e2
    show (XBEFORE e1 e2)       = show e1 ++ " << " ++ show e2
    show (XAFTER e1 e2)        = show e1 ++ " >> " ++ show e2
    show (XFNNOT e)            = "fn:not (" ++ show e ++ ")"
    show (XFNSUM e)            = "fn:sum (" ++ show e ++ ")"
    show (XFNCOUNT e)          = "fn:count (" ++ show e ++ ")"
    show (XFNEMPTY e)          = "fn:empty (" ++ show e ++ ")"
    show (XFNDIST e)           = "fn:distinct-values (" ++ show e ++ ")"
    show (XFNDDO e)            = "fn:distinct-doc-order (" ++ show e ++ ")"
    show (XFNDOC uri)          = "fn:doc (" ++ show uri ++ ")"
    show (XFNROOT e)           = "fn:root (" ++ show e ++ ")"
    show (XFNDATA e)           = "fn:data (" ++ show e ++ ")"
    show XFNTRUE               = "fn:true ()"
    show XFNFALSE              = "fn:false ()"
    show (XCASTINT e)          = "xs:integer (" ++ show e ++ ")"
    show (XCASTSTR e)          = "xs:string (" ++ show e ++ ")"
    show (XCASTDEC e)          = "xs:decimal (" ++ show e ++ ")"
    show (XCASTDBL e)          = "xs:double (" ++ show e ++ ")"
    show (XTYPESW e1 t e2 e3)  = "typeswitch (" ++ show e1 ++ ") case " ++ 
                                 show t ++ " return " ++ show e2 ++
                                 " default return " ++ show e3
    show (XLET v e1 e2)        = "let $" ++ v ++ " := " ++ show e1 ++ 
                                " return " ++ show e2
    show (XFOR v e1 e2)        = "for $" ++ v ++ " in " ++ show e1 ++ 
                                 " return " ++ show e2
    show (XFORAT v p e1 e2)    = "for $" ++ v ++ " at $" ++ p ++ " in " ++ 
                                 show e1 ++ " return " ++ show e2
    show (XORDER e1 es)        = show e1 ++ " order by " ++ 
                                 concat (intersperse ", " (map show es))
    show (XIF e1 e2 e3)        = "if " ++ show e1 ++ " then " ++ show e2 ++ 
                                 " else " ++ show e3
    show (XSEQ e1 e2)          = show e1 ++ ", " ++ show e2
    show (XFUN f es)           = f ++ " (" ++ 
                                 concat (intersperse ", " (map show es)) ++ ")"
    show (XELEM e1 e2)         = "element {" ++ show e1 ++ "} {" ++ 
                                 show e2 ++ "}"
    show (XTEXT e)             = "text {" ++ show e ++ "}"
    show (XPATH e (ax,kt,[]))  = show e ++ "/" ++ show ax ++ "::" ++ 
				 show kt ++ "()"
    show (XPATH e (ax,_,[n]))  = show e ++ "/" ++ show ax ++ "::" ++ n


-- XQuery sequence types

data SeqTy = Item Ty
           | (:?) Ty 
           | (:*) Ty 
           | (:+) Ty  

instance Show SeqTy where
    show (Item t) = show t
    show ((:?) t) = show t ++ "?"
    show ((:*) t) = show t ++ "*"
    show ((:+) t) = show t ++ "+"


-- XPath sublanguage 
-- (XPath axis specification in staircase join)

data XPaxis = Descendant                 -- XPath axes
            | Descendant_or_self
            | Child
            | Parent
              deriving (Ord, Eq)
type XPstep = (XPaxis, XMLkind, [QName]) -- XPath axis step

instance Show XPaxis where
    show Descendant         = "descendant"
    show Descendant_or_self = "descendant-or-self"
    show Child              = "child"
    show Parent             = "parent"
