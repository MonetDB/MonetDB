<book isbn="1-2345-6789-01">
  <author>John Doe</author>
  <title>A Nice Book</title>
</book>
--
let $foo := "bar" return
  <example>
    $foo is { $foo }
  </example>
--
<book isbn="{//orders/order[3]/@isbn}"/>
--
element book
{
  attribute isbn { "1-2345-6789-01" },
  element author { "John Doe" },
  element title { "A Nice Book" }
}
--
element { cast as xs:Qname (data(//element-names/myelement)) }
{
  element content { "My content" }
}
--
<sizes>{1,2,3}</sizes>
--
<mixture>{1, "2", "three"}</mixture>
--
<shoe size="7"/>
--
<shoe size="{7}"/>
--
<a b="{47, //salary}"/>
--
<pi>
  <?format role="output" ?>
</pi>
--
<xml-comment>
  <!-- An XML comment -->
  {-- An XQuery comment --}
</xml-comment>
--
<cdata-section><![CDATA[foo, <, & and > are cool!]]></cdata-section>
