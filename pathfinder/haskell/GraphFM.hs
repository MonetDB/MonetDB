module GraphFM (module Graph, 
                Gr (..), newNode, node', lab', suc') where

import Graph

import FiniteMap

data Gr a b = Gr (FiniteMap Node (Context a b))

instance Graph Gr where
    empty = Gr emptyFM

    c@(v,_,_) & (Gr g) = Gr (addToFM g v c)
 
    mkGraph cs = Gr (listToFM (zip (map node' cs) cs))
       
    contexts (Gr g) = eltsFM g

    context (Gr g) v = case lookupFM g v of
                       Just c  -> c
                       Nothing -> error ("no node " ++ show v ++ " in graph")

    nodes (Gr g) = keysFM g

    subgraph g v = mkGraph (closure g v)

    suc g v = suc' (context g v)

    lab (Gr g) v = lookupFM g v >>= return . lab'

    pre g v = map node' $ filter (elem v . suc') $ contexts $ g 


newNode :: Gr a b -> Node
newNode (Gr g) | sizeFM g == 0 = 1
               | otherwise     = maximum (keysFM g) + 1

node' :: Context a b -> Node
node' (v,_,_) = v

lab' :: Context a b -> a
lab' (_,l,_) = l

suc' :: Context a b -> [Node]
suc' (_,_,es) = map snd es

closure :: Gr a b -> Node -> [Context a b]
closure g v = context g v : concat (map (closure g) (suc g v))