declare function b($name as xs:string) as node()*
{ if ($name) then text{$name} else () };
declare function a($name as xs:string) as node()*
{ for $a in (1,2,3)
   for $s in b($name) return $s
};
a('file')
