<foo/>
--
default element namespace = "uri"
<foo/>
--
declare namespace ns = "uri"
<ns:foo/>
--
<bar xmlns:ns="x"><ns:foo/></bar>
--
<bar xmlns="x"><foo/></bar>
--
foo ()
--
xf:foo ()
--
default function namespace = "uri"
foo ()
--
define function foo ($i as xs:integer) as xs:boolean { $i lt 0 }
--
<xml:foo/>, <xs:foo/>, <xsd:foo/>, <xsi:foo/>, <xf:foo/>, <op:foo/>
--
declare namespace foo = "uri"
<foo:outer xmlns:foo="x"><foo:inner/></foo:outer>
--
declare namespace ns1 = "foo"
declare namespace ns2 = "foo"
define function ns1:fun ($x) { $x }
ns2:fun (42)
--
default function namespace = "foo"
declare namespace ns = "foo"
define function fun ($x) { $x }
ns:fun (42)
