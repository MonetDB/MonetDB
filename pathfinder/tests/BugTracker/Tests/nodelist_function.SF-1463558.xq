declare function local:foo1($n as node()*) as node()*
{
        $n
};

declare function local:foo2($n as node()*) as node()*
{
        $n
};

let $t := <a><b/><c/><d/></a>
return (
  local:foo1($t/descendant-or-self::node()/child::*),
  local:foo2($t/child::*)
)
