{--

    Definition of source language XQuery Core and 
    XQuery Core to relational algebra compiler.

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

module Main where

import Algb (Algb (..), 
             _PLUS, _TIMES, _MINUS, _DIV, _IDIV, _MOD,
             _GT, _EQ, _NOT, _AND, _OR, _NEG,
             RelID, Attr, Op, Proj, Numb, Arg, Args, Part, Pred, Tuple,
             Rel (..), Type,
             schm, attrs, types, extn, idx, proj, keys,
             XPaxis (..), XPkind (..), XPname, XPstep)
import Item  (Item (..), unN, unI)
import Ty (Ty (..), tyunion, tycommon)
import XMark (auction_xml, _elem, _text, _doc)
import Dag (dag, dot)

import List  (intersperse, nub, elemIndex, 
              sortBy, groupBy, (\\), genericLength)
import Numeric (showFFloat)
import NumExts (floatToDouble, doubleToFloat)

----------------------------------------------------------------------
-- Utilities

map1 :: (a -> a) -> [a] -> [a]
map1 f []     = []
map1 f (x:xs) = (f x):xs

----------------------------------------------------------------------
-- XQuery Core

type Var = String               -- XQuery variable $v
type Fun = String               -- XQuery function name
type URI = String               -- XML document URI (fn:doc)

data SeqTy = Item Ty
           | (:?) Ty 
           | (:*) Ty 
           | (:+) Ty  

instance Show SeqTy where
    show (Item t) = show t
    show ((:?) t) = show t ++ "?"
    show ((:*) t) = show t ++ "*"
    show ((:+) t) = show t ++ "+"

data XCore = XINT     Integer                   -- 42
           | XSTR     String                    -- "foo"
           | XDBL     Double                    -- 4.2e1
           | XDEC     Float                     -- 4.20
           | XEMPTY                             -- ()
	   | XVAR     Var                       -- $x
           | XPLUS    XCore XCore               -- e1 + e2
           | XTIMES   XCore XCore               -- e1 * e2
           | XMINUS   XCore XCore               -- e1 - e2
           | XDIV     XCore XCore               -- e1 div e2
           | XIDIV    XCore XCore               -- e1 idiv e2
           | XMOD     XCore XCore               -- e1 mod e2
           | XLT      XCore XCore               -- e1 < e2
           | XGT      XCore XCore               -- e1 > e2
           | XEQ      XCore XCore               -- e1 = e2
           | XOR      XCore XCore               -- e1 and e2
           | XAND     XCore XCore               -- e1 or e2
           | XNEG     XCore                     -- - e
           | XIS      XCore XCore               -- e1 is e2
           | XBEFORE  XCore XCore               -- e1 << e2
           | XAFTER   XCore XCore               -- e1 >> e2
           | XFNNOT   XCore                     -- fn:not (e)
           | XFNSUM   XCore                     -- fn:sum (e)
           | XFNCOUNT XCore                     -- fn:count (e)
           | XFNEMPTY XCore                     -- fn:empty (e)
           | XFNDIST  XCore                     -- fn:distinct-values (e)
           | XFNDDO   XCore                     -- fn:distinct-doc-order (e)
           | XFNDOC   URI                       -- fn:doc (uri)
           | XFNDATA  XCore                     -- fn:data (e)
           | XFNLAST                            -- fn:last ()
           | XFNPOS                             -- fn:positon ()
           | XFNTRUE                            -- fn:true ()
           | XFNFALSE                           -- fn:false ()
           | XFNROOT  XCore                     -- fn:root (e)
           | XCASTINT XCore                     -- e as xs:integer
           | XCASTSTR XCore                     -- e as xs:string 
           | XCASTDEC XCore                     -- e as xs:decimal
           | XCASTDBL XCore                     -- e as xs:double
           | XTYPESW  XCore SeqTy XCore XCore   -- typeswitch (e1)
                                                --    case t  return e2
                                                --    default return e3
	   | XLET     Var XCore XCore           -- let $x := e1 return e2
	   | XFOR     Var XCore XCore           -- for $x in e1 return e2
	   | XFORAT   Var Var XCore XCore       -- for $x at $p in e1 return e2
           | XORDER   XCore [XCore]             -- e1 order by e2, e3, ..., en
           | XIF      XCore XCore XCore         -- if e1 then e2 else e3
	   | XSEQ     XCore XCore               -- e1, e2
	   | XFUN     Fun [XCore]               -- f (e1, e2, ..., en)
           | XELEM    XCore XCore               -- element {e1} {e2}
           | XTEXT    XCore                     -- text {e}
           | XPATH    XCore XPstep              -- e / ax::n

instance Show XCore where
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
    show XFNLAST               = "fn:last ()"
    show XFNPOS                = "fn:position ()"
    show XFNTRUE               = "fn:true ()"
    show XFNFALSE              = "fn:false ()"
    show (XCASTINT e)          = "xs:integer (" ++ show e ++ ")"
    show (XCASTSTR e)          = "xs:string (" ++ show e ++ ")"
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
    show (XPATH e (ax,kt,[]))  = show e ++ "/" ++ show ax ++ show kt
    show (XPATH e (ax,_,[n]))  = show e ++ "/" ++ show ax ++ "::" ++ n

----------------------------------------------------------------------
-- XQuery Core -> Algebra

--            v |->( qv , [live])
type Env  = [(Var, (Algb, [Algb]))]  -- variable environment
type Funs = [(Fun, ([Var], XCore))]  -- function definitions

-- algebra expression computing XQuery xq
compile :: XCore -> Algb
compile xq = q
    where
    (q, _) = algebra xq

-- new live nodes generated by XQuery xq
compile_live :: XCore -> Algb
compile_live xq = live_nodes lns
    where 
    (_, lns) = algebra xq

-- algebraic expression computing the currently live nodes
-- from a list of live node fragment expressions
live_nodes :: [Algb] -> Algb
live_nodes []  = TBL (schm_doc) ([]::[Tuple])
live_nodes lns = foldr1 U lns 

-- compile XQuery Core expression xq into an algebraic expression
-- plus the list of algebraic expressions computing the live node fragments
algebra :: XCore -> (Algb, [Algb])
algebra xq = t [] (TBL [("iter",[NAT])] [[O 1]]) xq
    where	
    --   var    loop |-  e    => ( q  , [live])
    t :: Env -> Algb -> XCore -> (Algb, [Algb])

    -- (Const (int))
    t env loop (XINT i) = 
	(
	 X loop (TBL [("pos",[NAT]),("item",[INT])] [[O 1,I i]])
	,
	 []
	)

    -- (Const (str))
    t env loop (XSTR s) = 
	(
	 X loop (TBL [("pos",[NAT]),("item",[STR])] [[O 1,S s]])
	,
	 []
	)

    -- (Const (dec))
    t env loop (XDEC e) = 
	(
	 X loop (TBL [("pos",[NAT]),("item",[DEC])] [[O 1,E e]])
	,
	 []
	)

    -- (Const (dbl))
    t env loop (XDBL d) = 
	(
	 X loop (TBL [("pos",[NAT]),("item",[DBL])] [[O 1,D d]])
	,
	 []
	)

    -- (())
    t env loop XEMPTY =
        (
	 TBL [("iter",[NAT]),("pos",[NAT]),("item",[])] []
	,
         []
	)

    -- (Plus)
    t env loop (XPLUS e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _PLUS "res" ["item","item1"] (
	         JOIN ("iter","iter1") 
                     q1
                     (PROJ [("iter1","iter"),("item1","item")] q2)
             )
         )
	,
	 []
	)
	where
	(q1, []) = t env loop e1
	(q2, []) = t env loop e2

    -- (Times)
    t env loop (XTIMES e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _TIMES "res" ["item","item1"] (
	         JOIN ("iter","iter1") 
                      q1
                      (PROJ [("iter1","iter"),("item1","item")] q2)
             )
         )
	,
	 []
	)
	where
	(q1, []) = t env loop e1
	(q2, []) = t env loop e2

    -- (Minus)
    t env loop (XMINUS e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _MINUS "res" ["item","item1"] (
	         JOIN ("iter","iter1") 
                      q1
                      (PROJ [("iter1","iter"),("item1","item")] q2)
             )
         )
	,
	 []
	)
	where
	(q1, []) = t env loop e1
	(q2, []) = t env loop e2

    -- (Div)
    t env loop (XDIV e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _DIV "res" ["item","item1"] (
	         JOIN ("iter","iter1") 
                      q1
                      (PROJ [("iter1","iter"),("item1","item")] q2)
             )
         )
	,
	 []
	)
	where
	(q1, []) = t env loop e1
	(q2, []) = t env loop e2

    -- (IDiv)
    t env loop (XIDIV e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _IDIV "res" ["item","item1"] (
	         JOIN ("iter","iter1") 
                      q1
                      (PROJ [("iter1","iter"),("item1","item")] q2)
             )
         )
	,
	 []
	)
	where
	(q1, []) = t env loop e1
	(q2, []) = t env loop e2

    -- (Mod)
    t env loop (XMOD e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _MOD "res" ["item","item1"] (
	         JOIN ("iter","iter1") 
                      q1
                      (PROJ [("iter1","iter"),("item1","item")] q2)
             )
         )
	,
	 []
	)
	where
	(q1, []) = t env loop e1
	(q2, []) = t env loop e2

    -- (Less)
    t env loop (XLT e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _GT "res" ["item","item1"] (
	         JOIN ("iter","iter1") 
                      q2
                      (PROJ [("iter1","iter"),("item1","item")] q1)
             )
         )
	,
	 []
	)
	where
	(q1, _) = t env loop e1
	(q2, _) = t env loop e2

    -- (Greater)
    t env loop (XGT e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _GT "res" ["item","item1"] (
	         JOIN ("iter","iter1") 
                      q1
                      (PROJ [("iter1","iter"),("item1","item")] q2)
             )
         )
	,
	 []
	)
	where
	(q1, _) = t env loop e1
	(q2, _) = t env loop e2

    -- (Equal)
    t env loop (XEQ e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _EQ "res" ["item","item1"] (
  	         JOIN ("iter","iter1") 
                      q1
                      (PROJ [("iter1","iter"),("item1","item")] q2)
             )
         )
	,
	 []
	)
	where
	(q1, _) = t env loop e1
	(q2, _) = t env loop e2


    -- (Or)
    t env loop (XOR e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _OR "res" ["item","item1"] (
  	         JOIN ("iter","iter1") 
                      q1
                      (PROJ [("iter1","iter"),("item1","item")] q2)
             )
         )
	,
	 []
	)
	where
	(q1, _) = t env loop e1
	(q2, _) = t env loop e2

    -- (And)
    t env loop (XAND e1 e2) =
	(
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
             _AND "res" ["item","item1"] (
  	          JOIN ("iter","iter1") 
                       q1
                       (PROJ [("iter1","iter"),("item1","item")] q2)
             )
         )
	,
	 []
	)
	where
	(q1, _) = t env loop e1
	(q2, _) = t env loop e2

    -- (Neg)
    t env loop (XNEG e1) =
        (
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
  	     _NEG "res" "item" q1
         )
	,
	 []
	)
	where
	(q1, []) = t env loop e1

    -- (Not)
    t env loop (XFNNOT e1) =
        (
         PROJ [("iter","iter"),("pos","pos"),("item","res")] (
  	     _NOT "res" "item" q1
         )
	,
	 []
	)
	where
	(q1, []) = t env loop e1

    -- (Is)
    t env loop (XIS e1 e2) = t env loop (XEQ e1 e2)

    -- (Before)
    t env loop (XBEFORE e1 e2) = t env loop (XLT e1 e2)

    -- (After)
    t env loop (XAFTER e1 e2) = t env loop (XGT e1 e2)

    -- (Sum)
    t env loop (XFNSUM e1) =
        (
         X (U (SUM "item" "item" ["iter"] q1)
              (X (DIFF loop (PROJ [("iter","iter")] q1))
                 (TBL [("item",[INT])] [[I 0]])
              )
           )
           (TBL [("pos",[NAT])] [[O 1]])         
	,
	 []
	)
	where
	(q1, []) = t env loop e1

    -- (Count)
    t env loop (XFNCOUNT e1) =
        (
	 X (U (COUNT "item" ["iter"] q1)
              (X (DIFF loop (PROJ [("iter","iter")] q1))
                 (TBL [("item",[INT])] [[I 0]])
              )
           )
           (TBL [("pos",[NAT])] [[O 1]])
	,
	 []
	)
	where
	(q1, _) = t env loop e1

    -- (Empty)
    t env loop (XFNEMPTY e1) =
        (
         X (U (X (DIST (PROJ [("iter","iter")] q1))
                 (TBL [("item",[BOOL])] [[B False]])
              )
              (X (DIFF loop (PROJ [("iter","iter")] q1))
                 (TBL [("item",[BOOL])] [[B True]])
              )
           )
           (TBL [("pos",[NAT])] [[O 1]])
	,
	 []
	)
        where
	(q1, _) = t env loop e1

    -- (Distinct)
    t env loop (XFNDIST e1) =
	(
	 ROWNUM ("pos",[]) ["iter"] (
	     DIST (PROJ [("iter","iter"),("item","item")] q1)
         )
	,
 	 lns
	)
        where
	(q1, lns) = t env loop e1

    -- (Distinct-Doc-Order)
    t env loop (XFNDDO e1) =
	(
	 ROWNUM ("pos", ["item"]) ["iter"] (
	     DIST (PROJ [("iter","iter"),("item","item")] q1)
         )
	,
 	 lns
	)
        where
	(q1, lns) = t env loop e1

    -- (Doc)
    t env loop (XFNDOC uri) =
	(
	 X loop 
           (TBL [("pos",[NAT]),("item",[NODE])] [[O 1, root]])
	,
	 [doc]
	)
	where
        -- determine document relation and root node of document 
	-- referenced by uri
        doc  :: Algb
        root :: Item
	(doc, root) = case lookup uri documents of
		           Just d  -> d
		           Nothing -> error ("no document " ++ show uri ++ 
                                             " available")

    -- (Root)
    t env loop (XFNROOT e1) =
	(
	 X (PROJ [("iter","iter"),("item","pre")] (
                JOIN ("frag","frag1") 
                    (SEL "res" (
                         _EQ "res" ["level","zero"] (
                             X (live_nodes lns)
                               (TBL [("zero",[INT])] [[I 0]]))
			 )
                    )
                    (PROJ [("iter","iter"),("frag1","frag")] (
                         JOIN ("item","pre")
                              q1
                              (live_nodes lns)
                         )
                    )
                )
           )
           (TBL [("pos",[NAT])] [[O 1]])
	,
	 lns
	)
	where
	(q1, lns) = t env loop e1

    -- (Data)
    t env loop (XFNDATA e1) =
 	(
   	 PROJ [("iter","iter"),("pos","pos"),("item","prop")] (
             JOIN ("item","pre")
                  q1
                  (PROJ [("pre","pre"),("prop","prop")] (live_nodes lns))
         )
 	,
 	 []
 	)
        where 
 	(q1, lns) = t env loop e1

    -- (Last)
    t env loop XFNLAST = t env loop (XVAR "fs:last")

    -- (Pos)
    t env loop XFNPOS = t env loop (XVAR "fs:position")

    -- (True)
    t env loop XFNTRUE = 
	(
	 X loop (TBL [("pos",[NAT]),("item",[BOOL])] [[O 1,B True]])
	,
	 []
	)

    -- (False)
    t env loop XFNFALSE = 
	(
	 X loop (TBL [("pos",[NAT]),("item",[BOOL])] [[O 1,B False]])
	,
	 []
	)

    -- (CastInt)
    t env loop (XCASTINT e1) =
	(
	 CINT "item" q1
	,
	 []
	)
	where
	(q1, _) = t env loop e1

    -- (CastStr)
    t env loop (XCASTSTR e1) =
	(
	 CSTR "item" q1
	,
	 []
	)
	where
	(q1, _) = t env loop e1

    -- (CastDec)
    t env loop (XCASTDEC e1) =
	(
	 CDEC "item" q1
	,
	 []
	)
	where
	(q1, _) = t env loop e1

    -- (CastDbl)
    t env loop (XCASTDBL e1) =
	(
	 CDBL "item" q1
	,
	 []
	)
	where
	(q1, _) = t env loop e1

    -- (Typeswitch)
    t env loop (XTYPESW e1 ty e2 e3) =
	(
         U q2 q3
	,
	 lns2 ++ lns3
	)
	where
	(q1, lns1) = t env loop e1

        -- sequence type matching
        stm = seqtym ty
		
        loop2 = PROJ [("iter","iter")] (
                    SEL "type" stm)
        loop3 = PROJ [("iter","iter")] (
                    SEL "res" (_NOT "res" "type" stm))
	
        env2 = map lift2 env
	    where
	    lift2 (vi, (qvi, lnvi)) = 
		(vi, 
		 (PROJ [("iter","iter"),("pos","pos"),("item","item")] (
		    JOIN ("iter","iter1") qvi (PROJ [("iter1","iter")] loop2)),
                  lnvi
                 )
                )
        env3 = map lift3 env
	    where
	    lift3 (vi, (qvi, lnvi)) = 
		(vi, 
		 (PROJ [("iter","iter"),("pos","pos"),("item","item")] (
		    JOIN ("iter","iter1") qvi (PROJ [("iter1","iter")] loop3)),
                  lnvi
                 )
                )

        (q2, lns2) = t env2 loop2 e2
        (q3, lns3) = t env3 loop3 e3

        -- algebraic implementation of sequence type matching
        seqtym :: SeqTy -> Algb
        seqtym (Item ty) = 
          U (DIFF (DIST (PROJ [("iter","iter"),("type","type")] (
                      _AND "type" ["fst","type'"] (
                          _EQ "fst" ["pos","one"] (
                              X (TYPE  "type'" "item" ty q1)
                                (TBL [("one",[NAT])] [[O 1]])))))
                  )
                  (X (PROJ [("iter","iter")] (
                         SEL "res" (_NOT "res" "type" (
                             DIST (PROJ [("iter","iter"),("type","type")] (
                                _AND "type" ["fst","type'"] (
                                    _EQ "fst" ["pos","one"] (
                                        X (TYPE  "type'" "item" ty q1)
                                          (TBL [("one",[NAT])] [[O 1]])))))))))
                     (TBL [("type",[BOOL])] [[B True]])
                  )
            )
            (X (DIFF loop (PROJ [("iter","iter")] q1))
               (TBL [("type",[BOOL])] [[B False]])
            )
        seqtym ((:?) ty) = 
          U (DIFF (DIST (PROJ [("iter","iter"),("type","type")] (
                      _AND "type" ["fst","type'"] (
                          _EQ "fst" ["pos","one"] (
                              X (TYPE  "type'" "item" ty q1)
                                (TBL [("one",[NAT])] [[O 1]])))))
                  )
                  (X (PROJ [("iter","iter")] (
                         SEL "res" (_NOT "res" "type" (
                             DIST (PROJ [("iter","iter"),("type","type")] (
                                _AND "type" ["fst","type'"] (
                                    _EQ "fst" ["pos","one"] (
                                        X (TYPE  "type'" "item" ty q1)
                                          (TBL [("one",[NAT])] [[O 1]])))))))))
                     (TBL [("type",[BOOL])] [[B True]])
                  )
            )
            (X (DIFF loop (PROJ [("iter","iter")] q1))
               (TBL [("type",[BOOL])] [[B True]])
            )
        seqtym ((:+) ty) = 
          U (DIFF (DIST (PROJ [("iter","iter"),("type","type")] (
                            TYPE "type" "item" ty q1))
                  )
                  (X (PROJ [("iter","iter")] (
                          SEL "res" (_NOT "res" "type" (
                               DIST (PROJ [("iter","iter"),("type","type")] (
                                         TYPE "type" "item" ty q1))))))
                     (TBL [("type",[BOOL])] [[B True]])
                  )
            )
            (X (DIFF loop (PROJ [("iter","iter")] q1))
               (TBL [("type",[BOOL])] [[B False]])
            )
        seqtym ((:*) ty) = 
          U (DIFF (DIST (PROJ [("iter","iter"),("type","type")] (
                            TYPE "type" "item" ty q1))
                  )
                  (X (PROJ [("iter","iter")] (
                          SEL "res" (_NOT "res" "type" (
                               DIST (PROJ [("iter","iter"),("type","type")] (
                                         TYPE "type" "item" ty q1))))))
                     (TBL [("type",[BOOL])] [[B True]])
                  )
            )
            (X (DIFF loop (PROJ [("iter","iter")] q1))
               (TBL [("type",[BOOL])] [[B True]])
            )

    -- (Seq)
    t env loop (XSEQ e1 e2) = 
	(
	 PROJ [("iter","iter"),("pos","pos1"),("item","item")] (
	     ROWNUM ("pos1", ["ord","pos"]) ["iter"] (
	         U (X (TBL [("ord",[NAT])] [[O 0]]) q1) 
                   (X (TBL [("ord",[NAT])] [[O 1]]) q2)))
	,
         lns1 ++ lns2
        )
	where
	(q1, lns1) = t env loop e1
	(q2, lns2) = t env loop e2

    -- (Let)
    t env loop (XLET v e1 e2) =
	(
	 q2
	, 
	 lns2
	)
	where
	(q1, lns1) = t env loop e1
	(q2, lns2) = t ((v, (q1, lns1)):env) loop e2
	 
    -- (Var)
    t env loop (XVAR v) =
	(
	 qv
	,
	 lnv
	)
	where
	(qv, lnv) = case lookup v env of
		         Just q  -> q
	                 Nothing -> error ("variable $" ++ v ++ " unknown")

    -- (Apply)
    t env loop (XFUN f es) =
	(
	 qf
	,
	 lnsf
	)
	where
	(vs, ef) = case lookup f funs of
		        Just (vs, ef) -> (vs, ef)
		        Nothing       -> error ("function " ++ f ++ " unknown")

        env' = zip vs (map (t env loop) es) ++ env

        (qf, lnsf) = t env' loop ef

    -- (For)
    t env loop (XFOR v e1 e2) =
	(
	 PROJ [("iter","outer"), ("pos","pos1"), ("item","item")] (
             ROWNUM ("pos1", ["iter","pos"]) ["outer"] (
                 JOIN ("iter","inner") q2 map_
             )
         )
	,
         lns2
	)
	where
	(q1, lns1) = t env loop e1

        qv = X (TBL [("pos",[NAT])] [[O 1]]) 
               (PROJ [("iter","inner"), ("item","item")] (
                   ROWNUM ("inner", ["iter", "pos"]) [] q1))

        loop1 = PROJ [("iter","iter")] qv

        map_ = PROJ [("outer","iter"), ("inner","inner")] (
                    ROWNUM ("inner", ["iter", "pos"]) [] q1)
 
        -- extend environment with representations of variable v
        env' = [(v, (qv, lns1))] ++ map lift env
	   where
	   lift (vi, (qvi, lnvi)) = 
	       (vi, 
		(PROJ [("iter","inner"), ("pos","pos"), ("item","item")] (
		     JOIN ("iter","outer") qvi map_),
                 lnvi
                )
               )

        (q2, lns2) = t env' loop1 e2

    -- (ForAt)
    t env loop (XFORAT v p e1 e2) =
	(
	 PROJ [("iter","outer"), ("pos","pos1"), ("item","item")] (
             ROWNUM ("pos1", ["iter","pos"]) ["outer"] (
                 JOIN ("iter","inner") q2 map_
             )
         )
	,
         lns2
	)
	where
	(q1, lns1) = t env loop e1

        qv = X (TBL [("pos",[NAT])] [[O 1]]) 
               (PROJ [("iter","inner"), ("item","item")] (
                   ROWNUM ("inner", ["iter", "pos"]) [] q1))

        -- computes representation of variable that holds value
        -- of position variable p for this `for' loop
        qp = X (TBL [("pos",[NAT])] [[O 1]])
               (PROJ [("iter","inner"),("item","item")] (
                     (CINT "item" (
                          ROWNUM ("item", ["inner"]) ["outer"] map_))))
    
        loop1 = PROJ [("iter","iter")] qv

        map_ = PROJ [("outer","iter"), ("inner","inner")] (
                    ROWNUM ("inner", ["iter", "pos"]) [] q1)

        -- extend environment with representations of variables v and p
        env' = [(p, (qp, [])), 
                (v, (qv, lns1))] ++ map lift env
	   where
	   lift (vi, (qvi, lnvi)) = 
	       (vi, 
		(PROJ [("iter","inner"), ("pos","pos"), ("item","item")] (
		    JOIN ("iter","outer") qvi map_),
                 lnvi
                )
               )

        (q2, lns2) = t env' loop1 e2

    -- (Order)
    t env loop (XORDER e1 es) =
	(
         PROJ [("iter","ord"),("pos","pos"),("item","item")] (
             JOIN ("iter1","iter") 
                 q1
	         (PROJ [("ord","ord"),("iter1","iter1")] (
                     ROWNUM ("ord",items) [] order_specs)))  
	,
	 lns1
        )
	where
	(q1, lns1) = t env loop e1
        -- compile the order specs es (ignore any live node fragments)
	qs         = map (fst . t env loop) es

        -- generate unique column names
        iters =                   map ("iter" ++) (map show [1..])
        items = take (length qs) (map ("item" ++) (map show [1..]))
      
        qs' = map (\(q,(iter,item)) -> PROJ [(iter,"iter"),(item,"item")] q) 
                  (zip qs (zip iters items))
 
        order_specs :: Algb
        order_specs = fst (foldr1 (\(q,p) (join, _) -> (JOIN p q join, p))
                                  (zip qs' (zip iters (tail iters))))

    -- (If)
    t env loop (XIF e1 e2 e3) =
        (
         U q2 q3
	,
         lns2 ++ lns3
        )
        where
	(q1, _) = t env loop e1

        loop2 = PROJ [("iter","iter")] (SEL "item" q1)
        loop3 = PROJ [("iter","iter")] (SEL "res" (_NOT "res" "item" q1))

        env2 = map lift2 env
	    where
	    lift2 (vi, (qvi, lnvi)) = 
		(vi, 
		 (PROJ [("iter","iter"),("pos","pos"),("item","item")] (
		    JOIN ("iter","iter1") qvi (PROJ [("iter1","iter")] loop2)),
                  lnvi
                 )
                )
        env3 = map lift3 env
	    where
	    lift3 (vi, (qvi, lnvi)) = 
		(vi, 
		 (PROJ [("iter","iter"),("pos","pos"),("item","item")] (
		    JOIN ("iter","iter1") qvi (PROJ [("iter1","iter")] loop3)),
                  lnvi
                 )
                )

        (q2, lns2) = t env2 loop2 e2
        (q3, lns3) = t env3 loop3 e3

    -- (Elem)
    t env loop (XELEM e1 e2) =
	(
	 X (PROJ [("iter","iter"),("item","pre")] (roots n))
           (TBL [("pos",[NAT])] [[O 1]])
	,
	 [(PROJ (zip (attrs schm_doc) (attrs schm_doc)) n)] 
	)
	where
	(q1, _)   = t env loop e1
	(q2, lns) = t env loop e2

        n = ELEM (live_nodes lns) q1 q2
                                            
        roots n = SEL "res" (
                      _EQ "res" ["level","zero"] (
		          X n
                            (TBL [("zero",[INT])] [[I 0]])
                      )
                  )

    -- (Text)
    t env loop (XTEXT e1) =
	(
         X (PROJ [("iter","iter"),("item","pre")] n)
           (TBL [("pos",[NAT])] [[O 1]])
	,
	 [PROJ (zip (attrs (schm_doc)) (attrs (schm_doc))) n] 
	)
        where
	(q1, lns) = t env loop e1

        n = TEXT (live_nodes lns)
                 (U q1
                    (X (DIFF loop (PROJ [("iter","iter")] q1))
                       (TBL [("pos",[NAT]),("item",[STR])] [[O 1, S ""]])
                    )
                 )

    -- (Step)
    t env loop (XPATH e1 s) =
	(
         ROWNUM ("pos",["item"]) ["iter"] (
	      SCJ s (PROJ [("iter","iter"),("item","item")] q1)
                    (live_nodes lns)
         )
	 ,
	 lns
	)
        where
	(q1, lns) = t env loop e1

----------------------------------------------------------------------
-- Algebra evaluation


-- group relation r by attributes g
-- (group schema is identical to original relation schema)
group_by :: [Attr] -> Rel -> [[Tuple]]
group_by g r =
    (groupBy grp . sortBy ord) (extn r)
    where
    grp_key :: [a] -> [a]
    grp_key = keys g (attrs (schm r))

    ord :: Ord a => [a] -> [a] -> Ordering
    ord x y = compare (grp_key x) (grp_key y)
 
    grp :: Eq a => [a] -> [a] -> Bool
    grp x y = (grp_key x) == (grp_key y)


----------------------------------------------------------------------
-- evaluate algebraic expression,
-- given (next available pre, next available frag) 
evaluate :: Algb -> Rel
evaluate c = r
    where 
    (r, _) = eval (c, (next_pre, next_frag))

eval :: (Algb, (Item,Item)) -> (Rel, (Item,Item))
eval (ROWNUM (n,as) p c, (pre,frag)) = 
    (R ((n,[NAT]):schm r) (concat numbered), (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    groups :: [[Tuple]]
    groups = group_by p r

    skeys :: [a] -> [a]
    skeys = keys as (attrs (schm r))

    ord :: Ord a => [a] -> [a] -> Ordering
    ord x y = compare (skeys x) (skeys y)

    sorted :: [[Tuple]]
    sorted = map (sortBy ord) groups

    numbered = map (zipWith (:) (map O [1..])) sorted
    
eval (PROJ p c, (pre,frag)) =
    (R projschm (map proj (extn r)), (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))
       
    proj :: [a] -> [a]
    proj = keys (map snd p) (attrs (schm r))

    -- match (new) attribute names and types
    projschm = zip (map fst p) (types (proj (schm r)))

eval (SEL a c, (pre,frag)) =
    (R (schm r) sel, (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    skey :: [a] -> [a]
    skey = keys [a] (attrs (schm r))

    pred :: Tuple -> Bool
    pred = ([B True] ==) . skey
  
    sel = [ t | t <- extn r, pred t ]

eval (TYPE a1 a2 t c, (pre,frag)) =
    (R ((a1,[BOOL]):(reord (schm r))) res, (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    -- attributes of r reordered with attribute a2
    -- to be type-tested in the head
    reord :: [a] -> [a]
    reord = keys (a2:((attrs (schm r)) \\ [a2])) (attrs (schm r))
	 
    tytest :: Item -> Ty -> Item
    tytest (I _) INT  = B True
    tytest (S _) STR  = B True
    tytest (B _) BOOL = B True
    tytest (E _) DEC  = B True
    tytest (D _) DBL  = B True
    tytest (N _) NODE = B True
    tytest _     _    = B False
   
    -- perform type test on first attribute,
    -- result of test reflected by value of attribute a1
    res = map ((\(x:xs) -> (tytest x t):x:xs) . reord) (extn r)    
   
eval (OP1 _ t op a as c, (pre,frag)) =
    eval_op1 t op a as c (pre,frag)

eval (OP2 _ t op a as c, (pre,frag)) =
    eval_op2 t op a as c (pre,frag)

eval (SUM a s p c, (pre,frag)) =
    (R ((a,t):part (schm r)) res, (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    groups :: [[Tuple]]
    groups = group_by p r

    part :: [a] -> [a]
    part = keys p (attrs (schm r))

    arg :: [a] -> [a]
    arg = keys [s] (attrs (schm r))

    -- result type
    t :: [Ty]
    t = map tycommon (arg (types (schm r)))

    res = zipWith (:) (map (sum . map head . map arg) groups)
                      (map (part . head) groups)

eval (COUNT a p c, (pre,frag)) =
    (R ((a,[INT]):part (schm r)) res, (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    groups :: [[Tuple]]
    groups = group_by p r

    part :: [a] -> [a]
    part = keys p (attrs (schm r))

    res = zipWith (:) (map (I . genericLength) groups) 
                      (map (part . head) groups)

eval (U c1 c2, (pre,frag)) =
    (R uschm (extn r1 ++ map shuffle (extn r2)), (pre'',frag''))
    where
    (r1, (pre',frag'))   = eval (c1, (pre,frag))
    (r2, (pre'',frag'')) = eval (c2, (pre',frag'))

    -- shuffle attributes of r2 to match attribute order of r1
    shuffle :: [a] -> [a]
    shuffle = keys (attrs (schm r1)) (attrs (schm r2))

    -- union the types in each column
    uschm = zip (attrs (schm r1)) 
                (map tyunion (zip (types (schm r1)) 
                                  (types (shuffle (schm r2)))))
    
eval (DIFF c1 c2, (pre,frag)) =
    (R (schm r1) (extn r1 \\ map shuffle (extn r2)), (pre'',frag''))
    where
    (r1, (pre',frag'))   = eval (c1, (pre,frag))
    (r2, (pre'',frag'')) = eval (c2, (pre',frag'))

    -- shuffle attributes of r2 to match attribute order of r1
    shuffle :: [a] -> [a]
    shuffle = keys (attrs (schm r1)) (attrs (schm r2))

eval (DIST c, (pre,frag)) =
    (R (schm r) (nub (extn r)), (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

eval (X c1 c2, (pre,frag)) =
    (R (schm r1 ++ schm r2) product, (pre'',frag''))
    where
    (r1, (pre',frag'))   = eval (c1, (pre,frag))
    (r2, (pre'',frag'')) = eval (c2, (pre',frag'))

    product = [ x ++ y | x <- extn r1, y <- extn r2 ]

eval (JOIN (a1,a2) c1 c2, (pre,frag)) =
    (R (schm r1 ++ schm r2) join, (pre'',frag''))
    where
    (r1, (pre',frag'))   = eval (c1, (pre,frag))
    (r2, (pre'',frag'')) = eval (c2, (pre',frag'))

    jkeys :: [a] -> [a]
    jkeys = keys [a1,a2] (attrs (schm r1 ++ schm r2))

    pred :: Eq a => [a] -> Bool
    pred = (\[x,y] -> x == y) . jkeys

    join = [ t | x <- extn r1, y <- extn r2, let t = x ++ y, pred t ]
    
eval (CINT a c, (pre,frag)) =
    (R castedschm casted, (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    -- attributes of r reordered with attribute to be casted in the head
    reord :: [a] -> [a]
    reord = keys (a:((attrs (schm r)) \\ [a])) (attrs (schm r))

    -- cast first attribute to INT
    cast_INT :: Item -> Item
    cast_INT (I x)     = I x
    cast_INT (S x)     = I (floor (read x))
    cast_INT (B False) = I 0
    cast_INT (B True)  = I 1
    cast_INT (E x)     = I (truncate x) 
    cast_INT (D x)     = I (truncate x)
    cast_INT (O x)     = I x

    castedschm = (map1 (\(a,t) -> (a, [INT])) . reord) (schm r)

    casted = map (map1 cast_INT . reord) (extn r)

eval (CSTR a c, (pre,frag)) =
    (R castedschm casted, (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    -- attributes of r reordered with attribute to be casted in the head
    reord :: [a] -> [a]
    reord = keys (a:((attrs (schm r)) \\ [a])) (attrs (schm r))

    -- cast first attribute to STR
    cast_STR :: Item -> Item
    cast_STR (I x)     = S (show x)
    cast_STR (S x)     = S x
    cast_STR (B False) = S "false"
    cast_STR (B True)  = S "true"
    cast_STR (E x)     = S (showFFloat (Just 2) x "")
    cast_STR (D x)     = S (show x)

    castedschm = (map1 (\(a,t) -> (a, [STR])) . reord) (schm r)

    casted = map (map1 cast_STR . reord) (extn r)

eval (CDEC a c, (pre,frag)) =
    (R castedschm casted, (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    -- attributes of r reordered with attribute to be casted in the head
    reord :: [a] -> [a]
    reord = keys (a:((attrs (schm r)) \\ [a])) (attrs (schm r))

    -- cast first attribute to DEC
    cast_DEC :: Item -> Item
    cast_DEC (I x)     = E (fromInteger x)
    cast_DEC (S x)     = E (read x)
    cast_DEC (B False) = E 0.0
    cast_DEC (B True)  = E 1.0
    cast_DEC (E x)     = E x
    cast_DEC (D x)     = E (doubleToFloat x)

    castedschm = (map1 (\(a,t) -> (a, [DEC])) . reord) (schm r)

    casted = map (map1 cast_DEC . reord) (extn r)

eval (CDBL a c, (pre,frag)) =
    (R castedschm casted, (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    -- attributes of r reordered with attribute to be casted in the head
    reord :: [a] -> [a]
    reord = keys (a:((attrs (schm r)) \\ [a])) (attrs (schm r))

    -- cast first attribute to DBL
    cast_DBL :: Item -> Item
    cast_DBL (I x)     = D (fromInteger x)
    cast_DBL (S x)     = D (read x)
    cast_DBL (B False) = D 0.0
    cast_DBL (B True)  = D 1.0
    cast_DBL (E x)     = D (floatToDouble x)
    cast_DBL (D x)     = D x

    castedschm = (map1 (\(a,t) -> (a, [DBL])) . reord) (schm r)

    casted = map (map1 cast_DBL . reord) (extn r)

eval (SCJ s c1 c2, (pre,frag)) =
    (R [("iter",[NAT]),("item",[NODE])] ((concat . map staircase) contexts),
     (pre'',frag'')
    )
    where
    (r1, (pre',frag'))   = eval (c1, (pre,frag))
    (r2, (pre'',frag'')) = eval (c2, (pre',frag'))

    contexts :: [[Tuple]]
    contexts = group_by ["iter"] r1
    
    -- evaluate XPath step for each context set in turn
    staircase :: [Tuple] -> [Tuple]
    staircase c = extn res
	where
	-- iter value of context set c
        iter :: Item
	iter = (head . keys ["iter"] (attrs (schm r1))) (head c)

        -- retrieve nodes of this context set from ``live nodes'' c2
        context :: Rel
        (context, _) = eval (PROJ (zip (attrs (schm_doc)) 
                                       (attrs (schm_doc)))
                                  (JOIN ("item","pre") 
                                        (TBL (schm r1) c)
                                        c2),
                             (pre'',frag'')
                            )
                       
        -- evaluate XPath step s
	nodes :: Rel
        nodes = eval_xpath s context r2

        res :: Rel
        res = evaluate (X (TBL [("iter",[NAT])] [[iter]])
                          (PROJ [("item","pre")]
                                (TBL (schm nodes) (extn nodes))))

-- c1: live nodes, c2: tag, c3: content
eval (ELEM c1 c2 c3, (pre,frag)) = 
      -- remove old pre value, attach new pre value
    (R (schm_doc ++ [("iter",[NAT])]) 
       (zipWith (:) (map N [(unN pre'')..]) (map tail construct)),
     (pre'' + genericLength construct, frag'' + genericLength (extn r2))
    )
    where

    -- NB. evaluate the live nodes c1 and the element content c3
    --     using the same (pre,frag) values such that we can find
    --     the element content in the live nodes
    --     (in a DAG-based implementation, the node constructions in 
    --     c1 and c3 would be shared)
    (r1, (pre',frag'))   = eval (c1, (pre,frag))
    (r2, (pre'',frag'')) = eval (c2, (pre',frag'))
    (r3, _)              = eval (c3, (pre,frag)) 

    -- construct new element nodes
    construct :: [Tuple]
    construct = (concat . map cons) (extn r2)
        where
        -- construct a single new element node (t encodes iter and tag)
        cons :: Tuple -> [Tuple]
        cons t = extn (evaluate (X (TBL (schm_doc) (root:content))
                                   (TBL [("iter",[NAT])] [[iter]])))
            where
            -- iter value for this construction,
            -- tag name of new element node
            iter, tag :: Item
            [iter, tag] = (keys ["iter","item"] (attrs (schm r2))) t

            -- retrieve content root nodes for this new element node
            -- (pos ++ schm_doc)
            roots :: Rel
            roots = evaluate
		         (PROJ (("pos","pos"):zip (attrs (schm_doc)) 
                                                  (attrs (schm_doc)))
                             (JOIN ("pre","item")
                                   (TBL (schm_doc) (extn r1))
                                   (PROJ [("pos","pos"),("item","item")] 
                                        (JOIN ("iter1","iter") 
                                              (TBL [("iter1",[NAT])] [[iter]]) 
                                              (TBL (schm r3) (extn r3))))))
                                 
            pos, no_pos :: [a] -> [a]
            pos    = keys ["pos"]            (attrs (schm roots))
            no_pos = keys (attrs (schm_doc)) (attrs (schm roots))

            ord_pos :: Ord a => [a] -> [a] -> Ordering
            ord_pos x y = compare (pos x) (pos y)

            -- retrieve nodes in subtrees below content root nodes in order
            -- (project pos away after sorting)
            nodes :: [Tuple]
            nodes = subtrees (map no_pos (sortBy ord_pos (extn roots)))

            -- total number of nodes below new element node
            size :: Item
            size = I (genericLength nodes)

            -- update frag in nodes (schm_doc)
	    content :: [Tuple]
            content = map (\[p,s,l,k,t,f] -> [p,s,l,k,t,frag'' + iter - 1]) 
                          nodes

            -- new root node (schm_doc)
            root :: Tuple
            root = [N 0, size, I 0, I _elem, tag, frag'' + iter - 1] 

    -- concatenate nodes in all subtrees
    subtrees :: [Tuple] -> [Tuple]
    subtrees = (concat . map subtree) 
	where
        -- determine subtree below content root node in live nodes r1
        -- (and update level)
	subtree :: Tuple -> [Tuple]
        subtree root = map (\[p,s,l,k,t,f] -> [p,s,l - level + 1,k,t,f])  
                               (extn (eval_xpath (Descendant_or_self, Node, [])
                                                 (R (schm_doc) [root])
                                                 r1))
	    where
            -- level of content root node 
            level :: Item
            level = (head . keys ["level"] (attrs (schm_doc))) root

eval (TEXT c1 c2, (pre,frag)) =
    (R (schm_doc ++ [("iter",[NAT])])
       (map (keys (attrs (schm_doc) ++ ["iter"])
                  ["pre","frag","size","level","kind","prop","iter"]) tpre),
     (pre'' + genericLength tfrag, frag'' + genericLength tfrag)
    )
    where
    (_ , (pre',frag'))   = eval (c1, (pre,frag))
    (r2, (pre'',frag'')) = eval (c2, (pre',frag'))

    -- schm(texts) = size|level|kind|prop|iter
    texts :: Rel
    texts = evaluate
	         (X (TBL [("size",[INT]),("level",[INT]),("kind",[INT])] 
                         [[I 0, I 0, I _text]])
                    (PROJ [("prop","item"),("iter","iter")] 
                        (TBL (schm r2) (extn r2))))

    -- attach fragment identifiers
    -- schm(tfrag) = frag|size|level|kind|prop|iter
    tfrag :: [Tuple]
    tfrag = zipWith (:) (map I [(unI frag'')..]) (extn texts)

    -- attach pre values
    -- schm(tpre) = pre|frag|size|level|kind|prop|iter
    tpre :: [Tuple]
    tpre = zipWith (:) (map N [(unN pre'')..]) tfrag    

eval (TBL a t, (pre,frag)) =
    (R a t, (pre,frag))

eval (REL r, (pre,frag)) = 
    case lookup r rels of
	 Just rel -> (rel, (pre,frag))
         Nothing  -> error ("relation " ++ r ++ " unknown")

eval_op1 ty op a a1 c (pre,frag) =
    (R ((a, t):(arg (schm r))) res, (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    -- result type
    t :: [Ty]
    t = ty ((keys [a1] (attrs (schm r))) (types (schm r)))

    -- attributes of r reordered with argument to op in the head
    arg :: [a] -> [a]
    arg = keys (a1:((attrs (schm r)) \\ [a1])) (attrs (schm r))
    
    res = map ((\(x:xs) -> (op x):x:xs) . arg) (extn r)

eval_op2 ty op a [a1,a2] c (pre,frag) =
    (R ((a, t):(args (schm r))) res, (pre',frag'))
    where
    (r, (pre',frag')) = eval (c, (pre,frag))

    -- result type
    t :: [Ty]
    t = ty ((keys [a1,a2] (attrs (schm r))) (types (schm r)))

    -- attributes of r reordered with arguments to op in the head
    args :: [a] -> [a]
    args = keys ([a1,a2] ++ ((attrs (schm r)) \\ [a1,a2])) (attrs (schm r))

    res = map ((\(x:y:xs) -> (x `op` y):x:y:xs) . args) (extn r)


----------------------------------------------------------------------
-- XML tree encoding (pre|size|level|kind|prop|frag)
-- (NB. do NOT reorder the attributes of the encoding)

-- relation schema of document relations
schm_doc :: Type
schm_doc = [("pre",[NODE]),("size",[INT]),("level",[INT]),
            ("kind",[INT]),("prop",[STR]),("frag",[INT])]       

----------------------------------------------------------------------
-- XPath implementation

-- sort in document order and eliminate duplicates
sidoaed :: Rel -> Rel
sidoaed r = R (schm r) ((nub . sortBy docord) (extn r))
    where
    pre :: [a] -> [a]
    pre = keys ["pre"] (attrs (schm r))

    docord :: Ord a => [a] -> [a] -> Ordering
    docord x y = compare (pre x) (pre y)

-- XPath axes semantics

--         context   doc
--          node     node
type Axis = Tuple -> Tuple -> Bool

--       axis  context  doc
--               set
xpath :: Axis -> Rel -> Rel -> Rel
xpath ax c d = 
    sidoaed (R (schm d) ((concat . map (axis ax)) (extn c)))
    where
    -- access properties of document nodes v in well-defined order
    v_props :: [a] -> [a]
    v_props = keys ["pre","size","level","frag"] (attrs (schm d))

    -- access properties of context nodes c in well-defined order
    c_props :: [a] -> [a]
    c_props = keys ["pre","size","level","frag"] (attrs (schm c))
    
    -- filter document nodes according to axis predicate ax, context node cn
    axis ax cn = filter (ax (c_props cn) . v_props) (extn d)  

descendant :: Axis
descendant [c_pre, c_size, _, c_frag] [v_pre, _, _, v_frag] =
    v_pre > c_pre && v_pre <= c_pre + c_size && v_frag == c_frag    

descendant_or_self :: Axis
descendant_or_self [c_pre, c_size, _, c_frag] [v_pre, _, _, v_frag] =
    v_pre >= c_pre && v_pre <= c_pre + c_size && v_frag == c_frag    

ancestor :: Axis
ancestor [c_pre, c_size, _, c_frag] [v_pre, v_size, _, v_frag] =
    v_pre < c_pre && c_pre <= v_pre + v_size && v_frag == c_frag

following :: Axis
following [c_pre, c_size, _, c_frag] [v_pre, _, _, v_frag] =
    v_pre > c_pre + c_size && v_frag == c_frag

preceding :: Axis
preceding [c_pre, _, _, c_frag] [v_pre, v_size, _, v_frag] =
    v_pre + v_size < c_pre && v_frag == c_frag

child :: Axis
child c@[_, _, c_level, _] v@[_, _, v_level, _] =
    descendant c v && v_level == c_level + 1
 
-- XPath kind/name tests

kindt :: Integer -> Rel -> Rel
kindt k r = R (schm r) (filter ((I k == ) . head . kind) (extn r))
    where
    kind :: [a] -> [a]
    kind = keys ["kind"] (attrs (schm r))

namet :: String -> Rel -> Rel
namet n r = R (schm r) (filter ((S n ==) . head . name) (extn r))
    where
    name :: [a] -> [a]
    name = keys ["prop"] (attrs (schm r))

-- XPath step evaluation

kind :: XPkind -> Integer
kind Elem = _elem
kind Text = _text

axis :: XPaxis -> Axis
axis Descendant         = descendant
axis Descendant_or_self = descendant_or_self
axis Ancestor           = ancestor
axis Following          = following
axis Preceding          = preceding
axis Child              = child

--             step   context  doc
eval_xpath :: XPstep -> Rel -> Rel -> Rel
eval_xpath (ax, Node, []) c d =                 (xpath (axis ax) c d)
eval_xpath (ax, kt,   []) c d = 
                             kindt (kind kt)    (xpath (axis ax) c d)
eval_xpath (ax, _,   [n]) c d = 
                  (namet n . kindt (kind Elem)) (xpath (axis ax) c d)


----------------------------------------------------------------------
-- Serialization

-- serialize the evaluation results for query xq (in pos order)
serialize :: XCore -> String
serialize xq = (foldr (.) id . intersperse (showString " ")) 
                   (map (serial . head . item) (sortBy ord_pos (extn (res)))) 
                   "\n"
    where
    -- compile and evaluate query xq, 
    -- then serialize resulting items
    res  = evaluate (compile xq)
    -- determine the live nodes resulting from the evaluation of xq
    live = evaluate (compile_live xq)

    item, pos :: [a] -> [a]
    item = keys ["item"] (attrs (schm res))
    pos  = keys ["pos"]  (attrs (schm res))

    ord_pos :: Ord a => [a] -> [a] -> Ordering
    ord_pos x y = compare (pos x) (pos y)

    -- serialize a single item
    serial :: Item -> ShowS
    serial (I i)     = showString (show i)
    serial (S s)     = showString (show s)
    serial (B False) = showString "false"
    serial (B True)  = showString "true"
    serial (E e)     = showFFloat (Just 2) e
    serial (D d)     = showString (show d)
    serial (N p)     = node_serialize (N p) live

-- serialize the given node n
node_serialize :: Item -> Rel -> ShowS
node_serialize n live = 
    xml_serialize (extn (eval_xpath (Descendant_or_self,Node,[]) en live))
    where
    -- retrieve encoding for node n from live nodes
    en :: Rel
    en = evaluate (PROJ (zip (attrs (schm_doc))
                             (attrs (schm_doc)))
                        (JOIN ("item","pre") 
                              (TBL [("item",[NODE])] [[n]]) 
                              (TBL (schm live) (extn live))))

    -- serialize list of encoded nodes 
    xml_serialize :: [Tuple] -> ShowS
    xml_serialize []    = id
    xml_serialize ([N _, I 0   , I _, I kind, S t, I _]:ns)  
	| kind == _elem = showChar '<' . showString t . showString "/>" .
                          xml_serialize ns
	| kind == _text = showString t .
                          xml_serialize ns
    xml_serialize ([N _, I size, I _, I kind, S t, I _]:ns) 
        | kind == _doc  = xml_serialize desc
	| kind == _elem = showChar '<'    . showString t . showChar '>' . 
                          xml_serialize desc . 
			  showString "</" . showString t . showChar '>' .
			  xml_serialize foll
	where
	(desc, foll) = splitAt (fromInteger size) ns
    
                                   
----------------------------------------------------------------------
-- Test

-- mapping document URIs to document relations and document root nodes
documents :: [(URI, (Algb, Item))]
documents = 
    [("a.xml",       (REL "a.xml",       N 0))
    ,("b.xml",       (REL "b.xml",       N 7))
    ,("auction.xml", (REL "auction.xml", N 14))
    ]

-- persistent relations
rels :: [(RelID, Rel)]
rels = 
    [("a.xml",
       -- a.xml:	   
       --       a	   
       --     / | \  
       --    b  d "e"
       --    |  |	   
       --    c "b"   
      R schm_doc
        [[N 0, I 6, I (-1), I _doc,  S "a.xml", I 0]        
	,[N 1, I 5, I 0,    I _elem, S "a",     I 0]        
	,[N 2, I 1, I 1,    I _elem, S "b",     I 0]        
	,[N 3, I 0, I 2,    I _elem, S "c",     I 0]        
	,[N 4, I 1, I 1,    I _elem, S "d",     I 0]        
	,[N 5, I 0, I 2,    I _text, S "b",     I 0]        
	,[N 6, I 0, I 1,    I _text, S "e",     I 0]
	]
     )
    ,("b.xml",
       -- b.xml:	 
       --       a	 
       --     / | \ 
       --    b  d  e
       --    |  |	 
       --    c  b   
      R schm_doc
        [[N 7,  I 6, I (-1), I _doc,  S "b.xml", I 1]      
	,[N 8,  I 5, I 0,    I _elem, S "a",     I 1]      
	,[N 9,  I 1, I 1,    I _elem, S "b",     I 1]      
        ,[N 10, I 0, I 2,    I _elem, S "c",     I 1]      
        ,[N 11, I 1, I 1,    I _elem, S "d",     I 1]      
        ,[N 12, I 0, I 2,    I _elem, S "b",     I 1]      
        ,[N 13, I 0, I 1,    I _elem, S "e",     I 1]
        ]
     )
    ,("auction.xml",
      R schm_doc
        auction_xml
     )
    ]

-- next available (pre, frag) values
next_pre, next_frag :: Item
next_pre  = N 789
next_frag = I 3


-- XQuery function definitions
funs = [-- define function convert ($v) { 2 * $v }
        ("convert", (["v"], 
                    XTIMES (XINT 2) (XVAR "v"))
        )
       ,-- poor man's positional access: e[n]
        -- define function nth ($e, $n) {
        --   for $i at $p in $e
        --   return if $p = $n 
        --          then $i
        --          else ()
        -- }
        ("nth", (["e","n"],
                XFORAT "i" "p" (XVAR "e")
                               (XIF (XEQ (XVAR "p") (XVAR "n"))
                                    (XVAR "i")
                                    XEMPTY
                               )
                )
        )
       ]


-- xq0:
-- for $x in (10,20,30)
-- return for $y in (100,$x)
--        return f ($x,$y)
xq0 = XFOR "x" (XSEQ (XINT 10) (XSEQ (XINT 20) (XINT 30))) 
               (XFOR "y" (XSEQ (XINT 100) (XVAR "x"))
                         (XFUN "f" [XVAR "x", XVAR "y"]))

-- xq1:
-- for $x in (10,20)
-- return let $y := fn:sum (100,$x)
--        return $y + $x
xq1 = XFOR "x" (XSEQ (XINT 10) (XINT 20))
               (XLET "y" (XFNSUM (XSEQ (XINT 100) (XVAR "x")))
                         (XPLUS (XVAR "y") (XVAR "x")))

-- xq2:
-- (0,2,0)
xq2 = XSEQ (XINT 0) (XSEQ (XINT 2) (XINT 0))

-- xq3:
-- for $x in (10,20,30)
-- return if ($x < 25) then ($x,$x*25) else ()
xq3 = XFOR "x" (XSEQ (XINT 10) (XSEQ (XINT 20) (XINT 30)))
               (XIF (XLT (XVAR "x") (XINT 25)) 
	            (XSEQ (XVAR "x") (XTIMES (XVAR "x") (XINT 25)))
                    XEMPTY
               )

-- xq4:
-- fn:empty (())
xq4 = XFNEMPTY XEMPTY

-- xq5:
-- for $x in (42,"a") 
-- return ($x,42)
xq5 = XFOR "x" (XSEQ (XINT 42) (XSTR "a"))
               (XEQ (XVAR "x") (XINT 42))

-- xq6:
-- 1 + 3
xq6 = XPLUS (XINT 1) (XINT 3)

-- xq7:
-- for $x i (10,20,30)
-- return fn:fount (if ($x < 25) then ($x,$x*25) else ())
xq7 = XFOR "x" (XSEQ (XINT 10) (XSEQ (XINT 20) (XINT 30)))
               (XFNCOUNT (XIF (XLT (XVAR "x") (XINT 25)) 
	                      (XSEQ (XVAR "x") (XTIMES (XVAR "x") (XINT 25)))
                              XEMPTY
                         )
               )

-- xq8:
-- for $x in (10,20,30)
-- return (if ($x < 25) then $x else 0) + 42
xq8 = XFOR "x" (XSEQ (XINT 10) (XSEQ (XINT 20) (XINT 30)))
               (XPLUS (XIF (XLT (XVAR "x") (XINT 25)) 
	                   (XVAR "x")
                           (XINT 0)
                      )
                      (XINT 42)
               )

-- xq9:
-- fn:count (1,2)
xq9 = XFNCOUNT (XSEQ (XINT 1) (XINT 2))

-- xq10:
-- for $x in (10,20,30)
-- return fn:empty (if ($x < 25) then ($x,$x*25) else ())
xq10 = XFOR "x" (XSEQ (XINT 10) (XSEQ (XINT 20) (XINT 30)))
                (XFNEMPTY (XIF (XLT (XVAR "x") (XINT 25)) 
	                       (XSEQ (XVAR "x") (XTIMES (XVAR "x") (XINT 25)))
                               XEMPTY
                          )
                )

-- xq11:
-- some $x in (1,2,3) satisfies $x > 1
xq11 = XFNNOT (XFNEMPTY (XFOR "x" (XSEQ (XINT 1) (XSEQ (XINT 2) (XINT 3)))
                                  (XIF (XGT (XVAR "x") (XINT 1))
                                       (XINT 1)
                                       XEMPTY
                                  )
                        )
              )

-- xq12:
-- every $x in (1,2,3) satisfies $x > 2
xq12 = XFNEMPTY (XFOR "x" (XSEQ (XINT 1) (XSEQ (XINT 2) (XINT 3)))
                          (XIF (XFNNOT (XGT (XVAR "x") (XINT 2)))
                               (XINT 1)
                               XEMPTY
                          )
                )

-- xq13:
-- fn:doc ("a.xml")/descendant-or-self::node()
xq13 = XPATH (XFNDOC "a.xml") (Descendant_or_self, Node, [])

-- xq14:
-- (element {"x"} { element {"y"} { () }, 
--                  element {"z"} { () } })/descendant-or-self::z
xq14 = XPATH (XELEM (XSTR "x") (XSEQ (XELEM (XSTR "y") XEMPTY) 
                                     (XELEM (XSTR "z") XEMPTY)))
             (Descendant_or_self, Node, ["z"])

-- xq15:
-- (element {"a"} { for $x in ("b","c","d") 
--                  return element {$x} { () } })/descendant-or-self::d
xq15 = XPATH (XELEM (XSTR "a") 
                    (XFOR "x" (XSEQ (XSTR "b") (XSEQ (XSTR "c") (XSTR "d")))
                              (XELEM (XVAR "x") XEMPTY)))
             (Descendant_or_self, Node, ["d"])

-- xq16:
-- text {"foo"}
xq16 = XPATH (XTEXT (XSTR "foo")) (Descendant_or_self, Text, [])

-- xq17:
-- for $x in fn:doc ("a.xml")/descendant_or_self::text()
-- return fn:data ($x)
xq17 = XFOR "x" (XPATH (XFNDOC "a.xml") (Descendant_or_self, Text, []))
                (XFNDATA (XVAR "x"))

-- xq18:
-- for $x at $p in (10,20,30)
-- return ($p, for $y at $q in (100,200,300)
--             return $q)
xq18 = XFORAT "x" "p" (XSEQ (XINT 10) (XSEQ (XINT 20) (XINT 30)))
           (XSEQ (XVAR "p") 
                 (XFORAT "y" "q" (XSEQ (XINT 100) (XSEQ (XINT 200) (XINT 300)))
                         (XVAR "q")))


-- xq19 (idea due to Michael Brundage on www-ql@w3.org):
-- let $unrelated := element {"y"} { () }, element {"y"} { () }
-- return let $related := (element {"x"} { $unrelated })/y
--        return nth ($unrelated, 1) << nth ($unrelated, 2),
--               nth ($related, 1) << nth ($related, 2)
xq19 = XLET "unrelated" (XSEQ (XELEM (XSTR "y") XEMPTY)
                              (XELEM (XSTR "y") XEMPTY))
            (XLET "related" (XPATH (XELEM (XSTR "x") (XVAR "unrelated"))      
                            (Child, Elem, ["y"]))
                   (XSEQ (XBEFORE (XFUN "nth" [(XVAR "unrelated"), (XINT 1)])  
                                  (XFUN "nth" [(XVAR "unrelated"), (XINT 2)])
                         )
                         (XBEFORE (XFUN "nth" [(XVAR "related"), (XINT 1)])  
                                  (XFUN "nth" [(XVAR "related"), (XINT 2)])
                         )
                   )
            )

-- xmark_Q1:
-- for $b in fn:doc ("auction.xml")/site/people/person
-- return if fn:data ($b/id/text()) = "person0"
--        then $b/name/text()
--        else ()
xmark_Q1 = XFOR "b" (XPATH (XPATH (XPATH (XFNDOC "auction.xml") 
                                         (Child, Elem, ["site"]))
                           (Child, Elem, ["people"]))
                    (Child, Elem, ["person"]))
                    (XIF (XEQ (XFNDATA (XPATH (XPATH (XVAR "b") 
                                                     (Child, Elem, ["id"]))
                                              (Child, Text, [])))
                              (XSTR "person0")
                         )
                         (XPATH (XPATH (XVAR "b") 
                                        (Child, Elem, ["name"]))
                                (Child, Text, []))
                         XEMPTY
                    )

-- xmark_Q2:
-- for $b in fn:doc("auction.xml")/site/open_auctions/open_auction
-- return element {"increase"} 
--                { for $x at $p in $b/bidder
--                  return if $p = 1 then $x/increase/text()
--                         else ()
--                }
xmark_Q2 = XFOR "b" (XPATH (XPATH (XPATH (XFNDOC "auction.xml")
                                         (Child, Elem, ["site"]))
                                  (Child, Elem, ["open_auctions"]))
                           (Child, Elem, ["open_auction"]))
                    (XELEM (XSTR "increase") 
                           (XFORAT "x" "p"
                                   (XPATH (XVAR "b") 
                                          (Child, Elem, ["bidder"]))
                                   (XIF (XEQ (XVAR "p") (XINT 1))
                                        (XPATH (XPATH (XVAR "x") 
                                                  (Child, Elem, ["increase"]))
                                               (Child, Text, []))
                                        XEMPTY
                                   )
                           )
                    )

-- xmark_Q5:
-- fn:count(for $i in fn:doc("auction.xml")/site/closed_auctions/closed_auction
--          return if xs:int(fn:data($i/price/text())) > 40
--                 then $i/price
--                 else ())
xmark_Q5 = XFNCOUNT 
             (XFOR "i" (XPATH (XPATH (XPATH (XFNDOC "auction.xml")
                                            (Child, Elem, ["site"]))
                                     (Child, Elem, ["closed_auctions"]))
                              (Child, Elem, ["closed_auction"]))
                       (XIF (XGT (XCASTINT (XFNDATA (XPATH (XPATH (XVAR "i")
                                                      (Child, Elem, ["price"]))
                                                 (Child, Text, [])))
                                 )
                                 (XINT 40)
                            )
                            (XPATH (XVAR "i") 
                                   (Child, Elem, ["price"]))
                            XEMPTY
                       )
             )

-- xmark_Q6:
-- for $b in fn:doc("auction.xml")/site/regions
-- return fn:count($b//item)
xmark_Q6 = XFOR "b" (XPATH (XPATH (XFNDOC "auction.xml")
                                  (Child, Elem, ["site"]))
                           (Child, Elem, ["regions"]))
                    (XFNCOUNT (XPATH (XPATH (XVAR "b")
                                            (Descendant_or_self, Node, []))
                                     (Child, Elem, ["item"]))
                    )

-- xmark_Q7:
-- for $p in fn:doc("auction.xml")/site
-- return fn:count($p//description) +
--        fn:count($p//annotation)  +
--        fn:count($p//email)
xmark_Q7 = XFOR "p" (XPATH (XFNDOC "auction.xml") 
                           (Child, Elem, ["site"]))
                    (XPLUS (XFNCOUNT (XPATH (XPATH (XVAR "p")
                                               (Descendant_or_self, Node, []))
                                            (Child, Elem, ["description"])))
                     (XPLUS (XFNCOUNT (XPATH (XPATH (XVAR "p")
                                                (Descendant_or_self, Node, []))
                                             (Child, Elem, ["annotation"])))
                            (XFNCOUNT (XPATH (XPATH (XVAR "p")
                                                (Descendant_or_self, Node, []))
                                             (Child, Elem, ["email"])))
                     )
                    )

-- xmark_Q8:
-- for $p in fn:doc("auction.xml")/site/people/person
-- return let $a := for $t in fn:doc("auction.xml")/site/closed_auctions/
--                                                              closed_auction
--                  return if fn:data($t/buyer/person/text()) = 
--                            fn:data($p/id/text())
--                         then $t
--                         else ()
--        return element {"item"} { element {"person"} { $p/name/text() },
--                                 text { xs:string(fn:count($a)) }
--                                }
xmark_Q8 =
 XFOR "p" (XPATH (XPATH (XPATH (XFNDOC "auction.xml")
                               (Child, Elem, ["site"]))
                        (Child, Elem, ["people"]))
                 (Child, Elem, ["person"]))
          (XLET "a" (XFOR "t" (XPATH (XPATH (XPATH (XFNDOC "auction.xml")
                                                   (Child, Elem, ["site"]))
                                            (Child, Elem, ["closed_auctions"]))
                                     (Child, Elem, ["closed_auction"]))
                              (XIF (XEQ (XFNDATA (XPATH (XPATH (XPATH (XVAR "t")
                                                      (Child, Elem, ["buyer"]))
                                                     (Child, Elem, ["person"]))
                                                    (Child, Text, []))
                                        )
                                        (XFNDATA (XPATH (XPATH (XVAR "p")
                                                        (Child, Elem, ["id"]))
                                                       (Child, Text, []))
                                        )
                                   )
                                   (XVAR "t")
                                   XEMPTY
                              )
                    )
                    (XELEM (XSTR "item")
                           (XSEQ (XELEM (XSTR "person")
                                        (XPATH (XPATH (XVAR "p")
                                                      (Child, Elem, ["name"]))
                                               (Child, Text, []))
                                 )
                                 (XTEXT (XCASTSTR (XFNCOUNT (XVAR "a")))
                                 )
                           )
                    )
          )

-- xmark_Q13:
-- for $i in fn:doc("auction.xml")/site/regions/australia/item
-- return element {"item"} { element {"name"} { $i/name/text() },
--                           $i/description
-- 
--                         }
xmark_Q13 = XFOR "i" (XPATH (XPATH (XPATH (XPATH (XFNDOC "auction.xml")
                                                 (Child, Elem, ["site"]))
                                          (Child, Elem, ["regions"]))
                                   (Child, Elem, ["australia"]))
                            (Child, Elem, ["item"]))
                     (XELEM (XSTR "item")
                            (XSEQ (XELEM (XSTR "name")
                                         (XPATH (XPATH (XVAR "i")
                                                       (Child, Elem, ["name"]))
                                                (Child, Text, []))
                                  )
                                  (XPATH (XVAR "i")
                                         (Child, Elem, ["description"]))
                            )
                     )

-- xmark_Q17:
-- for $p in fn:doc("auction.xml")/site/people/person
-- return if fn:empty ($p/homepage/text())
--        then element {"person"} { $p/name/text() }
--        else ()
xmark_Q17 = XFOR "p" (XPATH (XPATH (XPATH (XFNDOC "auction.xml")
                                          (Child, Elem, ["site"]))
                                   (Child, Elem, ["people"]))
                            (Child, Elem, ["person"]))
                     (XIF (XFNEMPTY (XPATH (XPATH (XVAR "p")
                                                  (Child, Elem, ["homepage"]))
                                           (Child, Text, []))
                          )
                          (XELEM (XSTR "person") 
                                 (XPATH (XPATH (XVAR "p")
                                               (Child, Elem, ["name"]))
                                        (Child, Text, []))
                          )
                          XEMPTY
                     )

-- xmark_Q18:
-- define function convert ($v) { 2 * $v }      (: see `funs' above :)
-- 
-- for $i in fn:doc("auction.xml")/site/open_auctions/open_auction
-- return convert(xs:int(fn:data($i/reserve/text())))
xmark_Q18 = XFOR "i" (XPATH (XPATH (XPATH (XFNDOC "auction.xml")
                                          (Child, Elem, ["site"]))
                                   (Child, Elem, ["open_auctions"]))
                            (Child, Elem, ["open_auction"]))
                     (XFUN "convert" 
                           [XCASTINT (XFNDATA (XPATH (XPATH (XVAR "i")
                                                    (Child, Elem, ["reserve"]))
                                                  (Child, Text, [])))
                           ]
                     )

jerome = XLET "cat" (XFNDOC "auction.xml")
              (XFOR "author" (XFNDIST (XPATH (XPATH (XVAR "cat")
                                                    (Child, Elem, ["book"]))
                                             (Child, Elem, ["author"])))
                     (XLET "books" (XFOR "book" (XPATH (XVAR "cat") 
                                                       (Child, Elem, ["book"]))
                                         (XIF (XEQ (XFNDATA (XPATH (XVAR "book")
                                                     (Child, Elem, ["author"])))
                                                   (XVAR "author")
                                              )
                                              (XVAR "book")
                                              XEMPTY
                                         )
                                   )
                           (XELEM (XSTR "total-sales") (
                               XSEQ (XELEM (XSTR "author") 
                                           (XVAR "author"))
                                    (XELEM (XSTR "count") 
                                           (XFNCOUNT (XVAR "books")))
                               )
                           )
                     )
              )


xq = XFOR "x" (XINT 10)
              (XFOR "y" (XINT 100)
                        (XIF (XLT (XVAR "x") (XINT 15))
                             (XPLUS (XVAR "x") (XVAR "y"))
                             XEMPTY
                        )
              )

xq' = XFOR "x" (XINT 10)
               (XIF (XLT (XVAR "x") (XINT 15))
                    (XFOR "y" (XINT 100)
                              (XPLUS (XVAR "x") (XVAR "y"))
                    )
                    XEMPTY
               )

formododd = XFOR "x" (XSEQ (XINT 1) (XSEQ (XINT 2) (XSEQ (XINT 3) (XINT 4))))
                     (XIF (XEQ (XMOD (XVAR "x") (XINT 2)) (XINT 0))
                          (XSTR "even")
                          (XSTR "odd")
                     )

lastpos = XLET "fs:ctxt" (XPATH (XPATH (XFNDOC "auction.xml") 
                                       (Descendant_or_self, Node, []))
                                (Child, Elem, ["description"]))
               (XLET "fs:last" (XFNCOUNT (XVAR "fs:ctxt"))
                     (XFORAT "fs:dot" "fs:position" (XVAR "fs:ctxt")
                             (XSEQ (XVAR "fs:dot") 
                                   (XVAR "fs:position")
                             )
                     )
               )

xqo = XFOR "x" (XSEQ (XDBL 20) (XDBL 10))
               (XORDER (XSEQ (XINT 1) (XVAR "x"))
                       [XVAR "x"]
               )

q = XFORAT "x" "p" (XSEQ (XINT 10) (XDBL 20))
           (XVAR "p")
    

main = do
          -- print the original XQuery Core input query
          --print xmark_Q2
          -- print algebra expression (DAG) 
          -- (comment all other lines if you want to use `make ps')
          --putStr (dot (compile xmark_Q2))
          -- print algebra expression (tree)
          --print (compile xmark_Q2)
          -- print result of algebraic evaluation 
          print (evaluate (compile xmark_Q2))
          -- compile and evaluate query, then serialize the result as XML
          --putStr (serialize xmark_Q2) 

