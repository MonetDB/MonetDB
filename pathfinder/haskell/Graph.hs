module Graph (Node, Context, Graph (..)) where

type Node = Int

type Context a b = (Node, a, [(b, Node)])

class Graph gr where
    empty :: gr a b
    (&) :: Context a b -> gr a b -> gr a b
    mkGraph :: [Context a b] -> gr a b
    contexts :: gr a b -> [Context a b]
    context :: gr a b -> Node -> Context a b
    nodes :: gr a b -> [Node]
    subgraph :: gr a b -> Node -> gr a b
    lab :: gr a b -> Node -> Maybe a
    suc :: gr a b -> Node -> [Node]
    pre :: gr a b -> Node -> [Node]
    
