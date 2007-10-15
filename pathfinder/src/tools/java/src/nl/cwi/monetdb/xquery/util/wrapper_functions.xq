module namespace wrapper = "xrpcwrapper";

declare namespace xrpcm = "http://monetdb.cwi.nl/XQuery";

declare function wrapper:add-type($val as item()) as node()
{
      typeswitch ($val)
        case xs:boolean return
          <xrpcm:atomic-value xsi:type="xs:boolean">
            {$val}
          </xrpcm:atomic-value>
        case xs:integer return
          <xrpcm:atomic-value xsi:type="xs:integer">
            {$val}
          </xrpcm:atomic-value>
        case xs:decimal return
          <xrpcm:atomic-value xsi:type="xs:decimal">
            {$val}
          </xrpcm:atomic-value>
        case xs:float return
        (: pathfinder does not support xs:float yet, so replace it
         : with xs:double :)
          <xrpcm:atomic-value xsi:type="xs:double">
            {$val}
          </xrpcm:atomic-value>
        case xs:double return
          <xrpcm:atomic-value xsi:type="xs:double">
            {$val}
          </xrpcm:atomic-value>
        case xs:anyURI return
          <xrpcm:atomic-value xsi:type="xs:anyURI">
            {$val}
          </xrpcm:atomic-value>
        case xs:string return
          <xrpcm:atomic-value xsi:type="xs:string">
            {$val}
          </xrpcm:atomic-value>
        case xs:untypedAtomic return
          <xrpcm:atomic-value xsi:type="xs:untypedAtomic">
            {$val}
          </xrpcm:atomic-value>
        case xs:anyAtomicType return
          <xrpcm:atomic-value xsi:type="xs:anyAtomicType">
            {$val}
          </xrpcm:atomic-value>
        case attribute() return
          <xrpcm:attribute>{$val}</xrpcm:attribute>
        case comment() return
          <xrpcm:comment>{$val}</xrpcm:comment>
        case document-node() return
          <xrpcm:document>{$val}</xrpcm:document>
        case element() return
          <xrpcm:element>{$val}</xrpcm:element>
        case processing-instruction() return
          <xrpcm:processing-instruction>
            {$val}
          </xrpcm:processing-instruction>
        case text() return
          <xrpcm:text>{$val}</xrpcm:text>
        case node() return
          <xrpcm:node>{$val}</xrpcm:node>
        default return
          <xrpcm:item>{$val}</xrpcm:item>
};

declare function wrapper:s2n($seq as item()*) as node()
{
  if(empty($seq)) then <xrpcm:sequence/>
  else
    <xrpcm:sequence> {
      for $val in $seq 
      return wrapper:add-type($val)
    } </xrpcm:sequence>
};

declare function wrapper:udf-s2n($seq as item()*,
                                 $qn as xs:string) as node()
{
    <xrpcm:sequence> {
      for $val in $seq
      return <xrpcm:element xsi:type="{$qn}">{$val}</xrpcm:element>
    } </xrpcm:sequence>
};

declare function wrapper:n2s($nsid as xs:string, $nodes as node()*) as item()*
{
    for $typenode in $nodes/child::*
    return
      if($typenode/name() = concat($nsid, ":atomic-value")) then
        if (string($typenode/@xsi:type) = "xs:boolean") then
            $typenode cast as xs:boolean
        else if (string($typenode/@xsi:type) = "xs:integer") then
            $typenode cast as xs:integer
        else if (string($typenode/@xsi:type) = "xs:decimal") then
            $typenode cast as xs:decimal
        else if (string($typenode/@xsi:type) = "xs:double") then
            $typenode cast as xs:double
        else if (string($typenode/@xsi:type) = "xs:string") then
            $typenode/text()
        else if (string($typenode/@xsi:type) = "xs:anySimpleType") then
            $typenode/text()
        else 
            $typenode/text()
      else if ($typenode/name() = concat($nsid, ":attribute")) then
        let $attr-name := $typenode/attribute::*[1]/name()
        return attribute {$attr-name} { $typenode/attribute::* }
      else if ($typenode/name() = concat($nsid, ":comment")) then
        comment {$typenode/comment()}
      else if ($typenode/name() = concat($nsid, ":document")) then
        document {$typenode/child::*}
      else if ($typenode/name() = concat($nsid, ":element") or
               $typenode/name() = concat($nsid, ":node")    or
               $typenode/name() = concat($nsid, ":anyNode") or
               $typenode/name() = concat($nsid, ":item")) then
        element {$typenode/child::*[1]/name()}
                {$typenode/child::*[1]/child::*}
      else if ($typenode/name() =
                concat($nsid, ":processing-instruction")) then
        let $pi := $typenode/processing-instruction()
        return processing-instruction { $pi/name() } {$pi}
      else if ($typenode/name() = concat($nsid, ":text")) then
        text {$typenode/text()}
      else ()
};
