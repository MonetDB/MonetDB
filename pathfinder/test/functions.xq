default function namespace = "http://www.myfunctions.com"

define function myfunction($foo as element foo*)
{
  for $i in $foo
    return $i/bar
}

myfunction(<foo><bar/><baz/></foo>)
--
define function depth($e as element) as xs:integer
{
  if (empty($e/node())) then 1
    else max(for $c in $e/node() return depth($c)) +1
}
--
define function no_of_students($stud as element students)
{
  count($stud/*)
}
--
define function price($p as element product of type catalog)
{
  $p/USPrice
}
--
one-argument-function((1, 2, 3))
--
three-argument-function(1, 2, 3)
--
two-argument-function(1, ())
--
two-argument-function((1, 2), 3)
--
define function xy($p as element)
{
  $p
}

define function abc($d as xs:string)
{
  $d
}

for $i in /foo return xy ($i), abc ($i)
--
fn:data (<foo/>)
