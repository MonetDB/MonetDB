{-# OPTIONS -fallow-overlapping-instances  -fallow-undecidable-instances  -fglasgow-exts #-}

{--

    Algebra evaluation (DAG-based).

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

module Eval (eval, 
	     RelFrag (..), rel, frags) where

import DMGraph
import Core
import Algb
import Item
import Ty

import Control.Monad.State
import List (sortBy, groupBy, genericLength, (\\), mapAccumL, nub)
import Numeric (showFFloat)
import NumExts (floatToDouble, doubleToFloat)

----------------------------------------------------------------------
-- DAG ``evaluation''

{-
   To evaluate an algebra DAG ag with root node n, call 

                 (r, (rg, p)) = runState (eval ag n) (empty, 0) .

   Evaluation happens by building an isomorphic DAG rg whose nodes are
   relations/fragments, not algebraic operators (isomorphic means that
   n is also the root of rg).  The overall evaluation result is the
   relation/fragment r (which is the same as the relational annotation
   of root n in rg)

   The evalution threads through an additional context (the Int 0
   above), p reflects the updated context after evaluation is complete
   (a useful context might be the next available preorder rank for
   node construction).

-}

-- evaluation of an algebra operator yields 
-- a list of XML fragments and/or a relation

data RelFrag a = Rel         Rel
               |    Frag [a]
               | RelFrag [a] Rel

instance (XML a b, Show a, Show [a]) => Show (RelFrag a) where
    show (Rel        r) = show r
    show (Frag    fs  ) = show fs
    show (RelFrag fs r) = show fs ++ show r

rel :: RelFrag a -> Rel
rel (Rel r)       = r
rel (RelFrag _ r) = r

frags :: RelFrag a -> [a]
frags (Frag fs)      = fs
frags (RelFrag fs _) = fs


-- algebra DAG evaluation 
-- (evaluate algebraic query q on the already partially evaluated
-- relation DAG rg)

eval :: (XML a Node) =>
        (DAG (RelFrag a), Node) -> Algb -> ((DAG (RelFrag a), Node), RelFrag a)
eval (rg, p) q = ((rg', p'), res)
    where 
    (res, (rg', p')) = runState (do_eval (dag q) (top q)) (rg, p)

do_eval :: (XML a Node) =>
           DAG AlgOp -> Node -> State (DAG (RelFrag a), Node) (RelFrag a)
do_eval ag n = 
    do -- get relation DAG constructed so far
       rg <- get_dag
       -- is node n already in the (isomorphic) relation DAG?
       case (lab rg n) of
            -- yes, simply return the relation annotation
            Just r -> return r
            -- no, obtain algebraic operator op and operator args
            Nothing -> do let (_, op, args) = context ag n
                          -- evaluate all args, this modifies the
                          -- relation DAG), get result relations rs
	   	  	  rs <- mapM (do_eval ag) (map snd args) 
                          -- evaluate operator op on evaluated args rs
                          r <- e op rs
                          -- add node n with relation annotation r
                          -- to relation DAG
                          modify_dag ((n, r, args) &)
                          -- return relation annotation
		 	  return r
  

-- evaluate operator op on given argument relations/fragments

e :: (XML a Node) =>
     AlgOp -> [RelFrag a] -> State (DAG (RelFrag a), Node) (RelFrag a)

e (ROWNUM (a,as) ps) [a0] =
    do let r0 = rel a0

           groups :: [[Tuple]] 
           groups = group_by ps r0

           skeys :: Tuple -> Tuple
           skeys = keys as (cols (schm r0))

           ord :: Tuple -> Tuple -> Ordering
           ord t1 t2 = compare (skeys t1) (skeys t2)

           sorted :: [[Tuple]]
           sorted = map (sortBy ord) groups

           numbered :: [[Tuple]]
	   numbered = map (zipWith (:) (map O [1..])) sorted

       return (Rel (R ((a,[NAT]):schm r0) (concat numbered)))
 
e (PROJ cs) [a0] =
    do let r0 = rel a0

           -- column projection
           proj :: [a] -> [a]
           proj = keys (map snd cs) (cols (schm r0))

           -- match (new) columns names and types
           projschm = zip (map fst cs) (types (proj (schm r0)))

       return (Rel (R projschm (map proj (extn r0))))

e (TYPE c1 c2 t) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with column to typetest first
           arg :: [a] -> [a]
           arg = keys (c2:(cols (schm r0) \\ [c2])) (cols (schm r0))
       
           tytest :: Item -> Ty -> Item
           tytest (I _) INT  = B True
           tytest (S _) STR  = B True
           tytest (B _) BOOL = B True
           tytest (E _) DEC  = B True
           tytest (D _) DBL  = B True
           tytest (N _) NODE = B True
           tytest _     _    = B False

           res :: [Tuple]
           res = map (\cs@(x:_) -> (tytest x t):cs) $ map arg $ extn r0

       return (Rel (R ((c1, [BOOL]):(arg (schm r0))) res))

e (SEL c) [a0] =
    do let r0 = rel a0
        
           -- test column
           skey :: [a] -> [a]
           skey = keys [c] (cols (schm r0))

           -- selection predicate `c = true'
           pred :: Tuple -> Bool
           pred = ([B True] ==) . skey

           -- perform selection
           sel :: [Tuple]
           sel = [ t | t <- extn r0, pred t ] 

       return (Rel (R (schm r0) sel))

e (NEG c1 c2) [a0] =
    do let r0 = rel a0

           -- result type 
           t :: [Ty]
           t = tyone ((keys [c2] (cols (schm r0))) (types (schm r0)))

           -- columns of r0 reordered, with argument to arith op first
           arg :: [a] -> [a]
           arg = keys (c2:(cols (schm r0) \\ [c2])) (cols (schm r0))

           -- arith op
           neg :: Item -> Item
           neg (I x) = I (negate x)
           neg (E x) = E (negate x)
           neg (D x) = D (negate x)
           
           -- perform arithmetics with first column
           res :: [Tuple] 
           res = map (\cs@(x:_) -> (neg x):cs) $ map arg $ extn r0

       return (Rel (R ((c1, t):(arg (schm r0))) res))

e (PLUS c [c1,c2]) [a0] =
    do let r0 = rel a0

           -- result type (also tests for identical numeric types)
           t :: [Ty]
           t = tyone ((keys [c1,c2] (cols (schm r0))) (types (schm r0)))

           -- columns of r0 reordered, with arguments to arith op first
           args :: [a] -> [a]
           args = keys ([c1,c2] ++ (cols (schm r0) \\ [c1,c2]))
		      (cols (schm r0))

           -- arith op
           plus :: Item -> Item -> Item
           (I x) `plus` (I y) = I (x + y)
           (E x) `plus` (E y) = E (x + y)
           (D x) `plus` (D y) = D (x + y)

           -- perform arithmetics with first/second column
           res :: [Tuple]
           res = map (\cs@(x:y:_) -> (x `plus` y):cs) $ map args $ extn r0

       return (Rel (R ((c, t):(args (schm r0))) res))

e (TIMES c [c1,c2]) [a0] =
    do let r0 = rel a0

           -- result type (also tests for identical numeric types)
           t :: [Ty]
           t = tyone ((keys [c1,c2] (cols (schm r0))) (types (schm r0)))

           -- columns of r0 reordered, with arguments to arith op first
           args :: [a] -> [a]
           args = keys ([c1,c2] ++ (cols (schm r0) \\ [c1,c2]))
		      (cols (schm r0))

           -- arith op
           times :: Item -> Item -> Item
           (I x) `times` (I y) = I (x * y)
           (E x) `times` (E y) = E (x * y)
           (D x) `times` (D y) = D (x * y)

           -- perform arithmetics with first/second column
           res :: [Tuple]
           res = map (\cs@(x:y:_) -> (x `times` y):cs) $ map args $ extn r0

       return (Rel (R ((c, t):(args (schm r0))) res))

e (MINUS c [c1,c2]) [a0] =
    do let r0 = rel a0

           -- result type (also tests for identical numeric types)
           t :: [Ty]
           t = tyone ((keys [c1,c2] (cols (schm r0))) (types (schm r0)))

           -- columns of r0 reordered, with arguments to arith op first
           args :: [a] -> [a]
           args = keys ([c1,c2] ++ (cols (schm r0) \\ [c1,c2]))
		      (cols (schm r0))

           -- arith op
           minus :: Item -> Item -> Item
           (I x) `minus` (I y) = I (x - y)
           (E x) `minus` (E y) = E (x - y)
           (D x) `minus` (D y) = D (x - y)

           -- perform arithmetics with first/second column
           res :: [Tuple]
           res = map (\cs@(x:y:_) -> (x `minus` y):cs) $ map args $ extn r0

       return (Rel (R ((c, t):(args (schm r0))) res))

e (IDIV c [c1,c2]) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with arguments to arith op first
           args :: [a] -> [a]
           args = keys ([c1,c2] ++ (cols (schm r0) \\ [c1,c2]))
		      (cols (schm r0))

           -- arith op
           (I x) `idiv` (I y) = I (truncate (fromInteger x / fromInteger y))
           (E x) `idiv` (E y) = I (truncate (x / y))
           (D x) `idiv` (D y) = I (truncate (x / y))

           -- perform arithmetics with first/second column
           res :: [Tuple]
           res = map (\cs@(x:y:_) -> (x `idiv` y):cs) $ map args $ extn r0

       return (Rel (R ((c, [INT]):(args (schm r0))) res))

e (DIV c [c1,c2]) [a0] =
    do let r0 = rel a0

           -- result type 
           t :: [Ty]
           t = divty ((keys [c1,c2] (cols (schm r0))) (types (schm r0)))
	       where
	       divty :: [[Ty]] -> [Ty]
               divty [[INT],[INT]] = [DEC]
               divty [[DEC],[DEC]] = [DEC]
               divty [[DBL],[DBL]] = [DBL]

           -- columns of r0 reordered, with arguments to arith op first
           args :: [a] -> [a]
           args = keys ([c1,c2] ++ (cols (schm r0) \\ [c1,c2]))
		      (cols (schm r0))

           -- arith op
           (I x) `div` (I y) = E (fromInteger x / fromInteger y)
           (E x) `div` (E y) = E (x / y)
           (D x) `div` (D y) = D (x / y)

           -- perform arithmetics with first/second column
           res :: [Tuple]
           res = map (\cs@(x:y:_) -> (x `div` y):cs) $ map args $ extn r0

       return (Rel (R ((c, t):(args (schm r0))) res))

e (MOD c [c1,c2]) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with arguments to arith op first
           args :: [a] -> [a]
           args = keys ([c1,c2] ++ (cols (schm r0) \\ [c1,c2]))
		      (cols (schm r0))

           -- arith op
           modulo :: Item -> Item -> Item
           (I x) `modulo` (I y) = I (x `mod` y)
 
           -- perform arithmetics with first/second column
           res :: [Tuple]
           res = map (\cs@(x:y:_) -> (x `modulo` y):cs) $ map args $ extn r0

       return (Rel (R ((c, [INT]):(args (schm r0))) res))

e (GRT c [c1,c2]) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with arguments to comp op first
           args :: [a] -> [a]
           args = keys ([c1,c2] ++ (cols (schm r0) \\ [c1,c2]))
                      (cols (schm r0))

           -- comp op
           gt :: Item -> Item -> Item
           (I x) `gt` (I y) = B (x > y)
           (S x) `gt` (S y) = B (x > y)
           (B x) `gt` (B y) = B (x > y)
           (E x) `gt` (E y) = B (x > y)
           (D x) `gt` (D y) = B (x > y)
           (N x) `gt` (N y) = B (x > y)

           -- perform comparison of first/second column
           res :: [Tuple]
           res = map (\cs@(x:y:_) -> (x `gt` y):cs) $ map args $ extn r0

       return (Rel (R ((c, [BOOL]):(args (schm r0))) res))

e (EQL c [c1,c2]) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with arguments to comp op first
           args :: [a] -> [a]
           args = keys ([c1,c2] ++ (cols (schm r0) \\ [c1,c2]))
                      (cols (schm r0))

           -- comp op
           eq :: Item -> Item -> Item
           (I x) `eq` (I y) = B (x == y)
           (S x) `eq` (S y) = B (x == y)
           (B x) `eq` (B y) = B (x == y)
           (E x) `eq` (E y) = B (x == y)
           (D x) `eq` (D y) = B (x == y)
           (N x) `eq` (N y) = B (x == y)

           -- perform comparison of first/second column
           res :: [Tuple]
           res = map (\cs@(x:y:_) -> (x `eq` y):cs) $ map args $ extn r0

       return (Rel (R ((c, [BOOL]):(args (schm r0))) res))

e (NOT c1 c2) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with argument to Boolean op first
           arg :: [a] -> [a]
           arg = keys (c2:(cols (schm r0) \\ [c2])) (cols (schm r0))

           -- Boolean op
           _not :: Item -> Item
           _not (B x) = B (not x)
           
           -- apply Boolean op to first column
           res :: [Tuple] 
           res = map (\cs@(x:_) -> (_not x):cs) $ map arg $ extn r0

       return (Rel (R ((c1, [BOOL]):(arg (schm r0))) res))

e (OR c [c1,c2]) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with arguments to Boolean op first
           args :: [a] -> [a]
           args = keys ([c1,c2] ++ (cols (schm r0) \\ [c1,c2]))
                      (cols (schm r0))

           -- Boolean op
           or :: Item -> Item -> Item
           (B x) `or` (B y) = B (x || y)

           -- apply Boolean op to first/second column
           res :: [Tuple]
           res = map (\cs@(x:y:_) -> (x `or` y):cs) $ map args $ extn r0

       return (Rel (R ((c, [BOOL]):(args (schm r0))) res))

e (AND c [c1,c2]) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with arguments to Boolean op first
           args :: [a] -> [a]
           args = keys ([c1,c2] ++ (cols (schm r0) \\ [c1,c2]))
                      (cols (schm r0))

           -- Boolean op
           and :: Item -> Item -> Item
           (B x) `and` (B y) = B (x && y)

           -- apply Boolean op to first/second column
           res :: [Tuple]
           res = map (\cs@(x:y:_) -> (x `and` y):cs) $ map args $ extn r0

       return (Rel (R ((c, [BOOL]):(args (schm r0))) res))

e (SUM c1 c2 ps) [a0] =
    do let r0 = rel a0
       
           groups :: [[Tuple]]
           groups = group_by ps r0

           part :: [a] -> [a]
           part = keys ps (cols (schm r0))

           arg :: [a] -> [a]
           arg = keys [c2] (cols (schm r0))

           _sum :: Item -> [Item] -> Item
           _sum z     []          = z
           _sum (I z) ((I x):xs)  = _sum (I (x + z))               xs
           _sum (I z) ((E x):xs)  = _sum (E (x + fromInteger z))   xs
           _sum (E z) ((I x):xs)  = _sum (E (fromInteger x + z))   xs
           _sum (E z) ((E x):xs)  = _sum (E (x + z))               xs
           _sum (I z) ((D x):xs)  = _sum (D (x + fromInteger z))   xs
           _sum (D z) ((I x):xs)  = _sum (D (fromInteger x + z))   xs
           _sum (E z) ((D x):xs)  = _sum (D (x + floatToDouble z)) xs
           _sum (D z) ((E x):xs)  = _sum (D (floatToDouble x + z)) xs
           _sum (D z) ((D x):xs)  = _sum (D (x + z))               xs

           -- result type 
           t :: [Ty]
           t = map tycommon ((keys [c2] (cols (schm r0))) (types (schm r0)))

           res :: [Tuple]
           res = zipWith (:) (map (_sum (I 0) . map head . map arg) groups)
                             (map (part . head)                     groups)

       return (Rel (R ((c1,t):part (schm r0)) res))

e (COUNT c ps) [a0] =
    do let r0 = rel a0
		
           groups :: [[Tuple]]
           groups = group_by ps r0

           part :: [a] -> [a]
           part = keys ps (cols (schm r0))
 
           res = zipWith (:) (map (I . genericLength) groups)
                             (map (part .head)        groups)

       return (Rel (R ((c,[INT]):part (schm r0)) res))

e (SEQTY1 c1 c2 ps) [a0] =
    do let r0 = rel a0
       
           groups :: [[Tuple]]
           groups = group_by ps r0

           part :: [a] -> [a]
           part = keys ps (cols (schm r0))

           arg :: [a] -> [a]
           arg = keys [c2] (cols (schm r0))

           ex1 :: Tuple -> Item
           ex1 [b]   = b
           ex1 (_:_) = B False

           res :: [Tuple]
           res = zipWith (:) (map (ex1 . map head . map arg) groups)
                             (map (part . head)              groups)

       return (Rel (R ((c1,[BOOL]):part (schm r0)) res))

e (ALL c1 c2 ps) [a0] =
    do let r0 = rel a0
       
           groups :: [[Tuple]]
           groups = group_by ps r0

           part :: [a] -> [a]
           part = keys ps (cols (schm r0))

           arg :: [a] -> [a]
           arg = keys [c2] (cols (schm r0))

           all :: Tuple -> Item
	   all = B . foldr1 (&&) . map unB

           res :: [Tuple]
           res = zipWith (:) (map (all . map head . map arg) groups)
                             (map (part . head)              groups)

       return (Rel (R ((c1,[BOOL]):part (schm r0)) res))

e U [a0,a1] =
    do let r0 = rel a0 
           r1 = rel a1
           
           -- shuffle columns of r1 to match column order of r0
           shuffle :: [a] -> [a]
           shuffle = keys (cols (schm r0)) (cols (schm r1))

           -- union the types in each column
           uschm = zip (cols (schm r0)) 
                       (map tyunion (zip (types (schm r0)) 
                                         (types (shuffle (schm r1)))))

       return (Rel (R uschm (extn r0 ++ map shuffle (extn r1))))

e DIFF [a0,a1] =
    do let r0 = rel a0 
           r1 = rel a1
           
           -- shuffle columns of r1 to match column order of r0
           shuffle :: [a] -> [a]
           shuffle = keys (cols (schm r0)) (cols (schm r1))

       return (Rel (R (schm r0) (extn r0 \\ map shuffle (extn r1))))

e DIST [a0] =
    do let r0 = rel a0
		
       return (Rel (R (schm r0) (nub (extn r0))))

e X [a0,a1] =
    do let r0 = rel a0 
           r1 = rel a1
       
           cross = [ x ++ y | x <- extn r0, y <- extn r1 ] 

       return (Rel (R (schm r0 ++ schm r1) cross))

e (JOIN (c1, c2)) [a0,a1] =
    do let r0 = rel a0
	   r1 = rel a1

           -- join keys
           jkeys :: Tuple -> Tuple
           jkeys = keys [c1,c2] (cols (schm r0 ++ schm r1))

           -- join predicate
           jpred :: Tuple -> Bool
           jpred = (\[x,y] -> x == y) . jkeys

           join = [ t | x <- extn r0, y <- extn r1, let t = x ++ y, jpred t ]

       return (Rel (R (schm r0 ++ schm r1) join))
 
e (CINT c) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with column to be casted first
           reord :: [a] -> [a]
           reord = keys (c:(cols (schm r0) \\ [c])) (cols (schm r0))

           -- cast item to type INT
           cast_INT :: Item -> Item
           cast_INT (I x)     = I x
           cast_INT (S x)     = I (floor (read x))
           cast_INT (B False) = I 0
           cast_INT (B True)  = I 1
           cast_INT (E x)     = I (truncate x)
           cast_INT (D x)     = I (truncate x)
           cast_INT (O x)     = I (toInteger x)

           res :: [Tuple]
           res = map (map1 cast_INT . reord) (extn r0)

       return (Rel (R ((c,[INT]):tail (reord (schm r0))) res))

e (CSTR c) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with column to be casted first
           reord :: [a] -> [a]
           reord = keys (c:(cols (schm r0) \\ [c])) (cols (schm r0))

           -- cast item to type STR
           cast_STR :: Item -> Item
           cast_STR (I x)     = S (show x)
           cast_STR (S x)     = S x
           cast_STR (B False) = S "false"
           cast_STR (B True)  = S "true"
           cast_STR (E x)     = S (showFFloat (Just 2) x "")
           cast_STR (D x)     = S (show x)

           res :: [Tuple]
           res = map (map1 cast_STR . reord) (extn r0)

       return (Rel (R ((c,[STR]):tail (reord (schm r0))) res))

e (CDEC c) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with column to be casted first
           reord :: [a] -> [a]
           reord = keys (c:(cols (schm r0) \\ [c])) (cols (schm r0))

           -- cast item to type DEC
           cast_DEC :: Item -> Item
           cast_DEC (I x)     = E (fromInteger x)
           cast_DEC (S x)     = E (read x)
           cast_DEC (B False) = E 0.0
           cast_DEC (B True)  = E 1.0
           cast_DEC (E x)     = E x
           cast_DEC (D x)     = E (doubleToFloat x)

           res :: [Tuple]
           res = map (map1 cast_DEC . reord) (extn r0)

       return (Rel (R ((c,[DEC]):tail (reord (schm r0))) res))

e (CDBL c) [a0] =
    do let r0 = rel a0

           -- columns of r0 reordered, with column to be casted first
           reord :: [a] -> [a]
           reord = keys (c:(cols (schm r0) \\ [c])) (cols (schm r0))

           -- cast item to type DBL
           cast_DBL :: Item -> Item
           cast_DBL (I x)     = D (fromInteger x)
           cast_DBL (S x)     = D (read x)
           cast_DBL (B False) = D 0.0
           cast_DBL (B True)  = D 1.0
           cast_DBL (E x)     = D (floatToDouble x)
           cast_DBL (D x)     = D x

           res :: [Tuple]
           res = map (map1 cast_DBL . reord) (extn r0)

       return (Rel (R ((c,[DBL]):tail (reord (schm r0))) res))

e ELEM [a0,a1,a2] =
    do p <- get_pre

       let fs0 = frags a0        -- live XML fragments
           r1  = rel a1          -- tags (one per iter)
           r2  = rel a2          -- element contents (0..n per iter)
 
	   iter = concat (map (keys ["iter"] (cols (schm r1))) (extn r1))
	   tags = concat (map (keys ["item"] (cols (schm r1))) (extn r1))
 
           citer = concat (map (keys ["iter"] (cols (schm r2))) (extn r2))
           cpos  = concat (map (keys ["pos"]  (cols (schm r2))) (extn r2))
           citem = concat (map (keys ["item"]  (cols (schm r2))) (extn r2))

           -- (iter, pos, subtree)
           -- pair iter,pos values with the subtree corresponding to the
           -- respective node ID
           ips = zip3 citer
                      cpos
                      (map (\n -> subtree (head $ filter (contains n) fs0) n) (map unN citem))

           contents = map content iter
               where
               cmp_pos x y = compare (snd3 x) (snd3 y)

               -- content XML fragments for element constructed in iteration i
               content i = map thd3               $      -- extract subtree
                           sortBy cmp_pos         $ 
                           filter ((== i) . fst3) $ ips

           (p', fs) = mapAccumL element p (zip (map unS tags) contents)

           pre = map (N . root) fs

           -- relational output
           -- schm(elems) = iter|pre
           elems :: Rel
           elems = R [("iter",[NAT]),("pre",[NODE])] (columns [iter,pre])

       modify_pre (const p')
       return (RelFrag fs elems)

e TEXT [a0] =
    do p <- get_pre

       let r0 = rel a0							   
	                                                                  
	   iter  = concat (map (keys ["iter"] (cols (schm r0))) (extn r0)) 
	   cdata = concat (map (keys ["item"] (cols (schm r0))) (extn r0)) 

           (p', fs) = mapAccumL text p (map unS cdata)
                                  
           pre = map (N . root) fs
                      
	   -- relational output					   
	   -- schm(texts) = iter|pre					   
	   texts :: Rel						   
	   texts = R [("iter",[NAT]),("pre",[NODE])] (columns [iter, pre])

       modify_pre (const p')
       return (RelFrag fs texts)

e (SCJ s) [a0,a1] =
    do let fs0 = frags a0          -- live XML fragments
           r1  = rel a1             -- context node sequences

	   iter = concat (map (keys ["iter"] (cols (schm r1))) (extn r1))
	   ctxt = concat (map (keys ["item"] (cols (schm r1))) (extn r1))
           
           -- (iter, ctxt, frag):
           -- pair context nodes with their respective iter and XML fragment
           -- (there is exactly one unique fragment which contains a node n)
           icf = zip3 iter
                      (map unN ctxt) 
                      (map (\n -> head $ filter (contains n) fs0) (map unN ctxt))
           -- relational output
           -- schm(nodes) = iter|item
           nodes :: Rel
           nodes = R [("iter",[NAT]),("item",[NODE])] 
                   (nub                                                 $
                    concat                                              $
                    map (\(i,c,f) -> map ((i:).(:[]).N) (step s f c)) $ icf)
               where
               step (ax,_,[nt])     t n = filter ((== nt)      . name t) $
                                          filter ((== XMLElem) . kind t) $ 
                                          (axis ax) t n    
               step (ax,XMLNode,[]) t n = (axis ax) t n    
               step (ax,kt,[])      t n = filter ((== kt)      . kind t) $ 
                                          (axis ax) t n    

               axis Parent             = parent
               axis Descendant         = descendant
               axis Descendant_or_self = descendant_or_self
               axis Child              = child
               axis Self               = \t n -> [n]

       return (Rel nodes)
    
e (TBL ty ts) _ = 
    return (Rel (R ty ts))

e DMROOTS [a0] =
    do let r0 = rel a0
       return (Rel r0)

e DMFRAGS [a0] =
    do let fs0 = frags a0
       return (Frag fs0)

e DMDOC [a0] =
    do p <- get_pre

       let r0 = rel a0

           iter = concat (map (keys ["iter"] (cols (schm r0))) (extn r0))
           uris = concat (map (keys ["item"] (cols (schm r0))) (extn r0)) 

           (p', fs) = mapAccumL doc p (map unS uris)

           -- IDs of root nodes of loaded fragments
           pre  = map (N . root) fs

           -- relational output
           -- schm(docs) = iter|pre
           docs :: Rel
           docs = R [("iter",[NAT]),("pre",[NODE])] (columns [iter,pre])

       modify_pre (const p')
       return (RelFrag fs docs)

e DMDATA [a0,a1] =
    do let fs0 = frags a0      -- live XML fragments
	   r1  = rel a1        -- XML nodes

	   iter = concat (map (keys ["iter"] (cols (schm r1))) (extn r1))
	   node = concat (map (keys ["item"] (cols (schm r1))) (extn r1))

           -- (iter, node, frag)
           -- pair nodes with their respective iter and XML fragment
           -- (there is exactly one unique fragment which contains a node n)
           inf = zip3 iter
                      (map unN node) 
                      (map (\n -> head $ filter (contains n) fs0) (map unN node))

           -- relational output
           -- schm(datas) = iter|item
           datas :: Rel
           datas = R [("iter",[NAT]),("item",[STR])]
                     (map (\(i,n,f) -> [i, S (string_value f n)]) inf)

       return (Rel datas)

e DMROOT [a0,a1] =
    do let fs0 = frags a0       -- live XML fragments
	   r1  = rel a1         -- XML nodes

	   iter = concat (map (keys ["iter"] (cols (schm r1))) (extn r1))
	   node = concat (map (keys ["item"] (cols (schm r1))) (extn r1))
 
           -- (iter, frag)
           -- pair iters with their respective iter and XML fragment
           if_ = zip iter
                     (map (\n -> head $ filter (contains n) fs0) (map unN node))

           -- relational output
           -- schm(roots) = iter|item
           roots :: Rel
           roots = R [("iter",[NAT]),("item",[STR])]
                     (map (\(i,f) -> [i, N (root f)]) if_)

       return (Rel roots)

e DMEMPTY _ =
    return (Frag [])    

e DMU [a0,a1] =
    do let fs0 = frags a0
           fs1 = frags a1

       return (Frag (fs0 ++ fs1))

e op _          =
    error (show op)

----------------------------------------------------------------------    
-- evaluation utilities

fst3 :: (a,b,c) -> a
fst3 (x,_,_) = x

snd3 :: (a,b,c) -> b
snd3 (_,y,_) = y

thd3 :: (a,b,c) -> c
thd3 (_,_,z) = z

map1 :: (a -> a) -> [a] -> [a]
map1 f []     = []
map1 f (x:xs) = (f x):xs

-- transpose list of lists (form tuples from list of rows),
-- shortest column list determines number of tuples (unlike Haskell's tranpose)
columns :: [[a]] -> [[a]]
columns xs | any null xs = []
           | otherwise   = (map head xs):(columns (map tail xs))

-- group relation r by columns g
-- (group schema is identical to original relation schema)
group_by :: [Col] -> Rel -> [[Tuple]]
group_by g r =
    (groupBy grp . sortBy ord) (extn r)
    where
    grp_key :: Tuple -> Tuple
    grp_key = keys g (cols (schm r))

    ord :: Tuple -> Tuple -> Ordering
    ord x y = compare (grp_key x) (grp_key y)
 
    grp :: Tuple -> Tuple -> Bool
    grp x y = (grp_key x) == (grp_key y)


----------------------------------------------------------------------
-- read/write evaluation state

get_dag :: State (DAG a, b) (DAG a)
get_dag = gets fst

get_pre :: State (DAG a, b) b
get_pre = gets snd

modify_dag :: (DAG a -> DAG a) -> State (DAG a, b) ()
modify_dag f = do (e, p) <- get
                  put (f e, p)

modify_pre :: (b -> b) -> State (DAG a, b) ()
modify_pre f = do (e, p) <- get
                  put (e, f p)
 
